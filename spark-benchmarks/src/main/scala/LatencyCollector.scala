/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spark.benchmark

import java.util.Map

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler._

class StopContextThread(ssc: StreamingContext) extends Runnable {
  def run {
    ssc.stop(true, true)
  }
}


class LatencyListener(ssc: StreamingContext, commonConfig: Map[String, Any]) extends StreamingListener {

  var metricMap: scala.collection.mutable.Map[String, Object] = _
  var startTime = 0L
  var endTime = 0L
  var totalDelay = 0L
  var hasStarted = false
  var batchCount = 0
  var totalRecords = 0L
  val thread: Thread = new Thread(new StopContextThread(ssc))


  def getMap(): scala.collection.mutable.Map[String, Object] = synchronized {
    if (metricMap == null) metricMap = scala.collection.mutable.Map()[String, Object]
    metricMap
  }

  def setMap(metricMap: scala.collection.mutable.Map[String, Object]) = synchronized {
    this.metricMap = metricMap
  }

  val batchSize = commonConfig.get("spark.performance.batchTime") match {
    case n: Number => n.longValue()
    case other => throw new ClassCastException(other + " not a Number")
  }
  val recordLimitPerThread = commonConfig.get("data.kafka.Loader.thread.recordLimit") match {
    case n: Number => n.longValue()
    case other => throw new ClassCastException(other + " not a Number")
  }
  val loaderThreads = commonConfig.get("data.kafka.Loader.thread") match {
    case n: Number => n.intValue()
    case other => throw new ClassCastException(other + " not a Number")
  }

  val recordLimit = loaderThreads * recordLimitPerThread

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    val batchInfo = batchCompleted.batchInfo
    val prevCount = totalRecords
    var recordThisBatch = batchInfo.numRecords

    if (!thread.isAlive) {
      totalRecords += recordThisBatch
      val imap = getMap
      imap(batchInfo.batchTime.toString()) = "batchTime" + batchInfo.batchTime +
        ",batch Count so far" + batchCount +
        ".total Records so far" + totalRecords +
        ",record This Batch" + recordThisBatch +
        ",submission Time" + batchInfo.submissionTime +
        ",processing Start Time" + batchInfo.processingStartTime +
        ",processing End Time" + batchInfo.processingEndTime +
        ",scheduling Delay" + batchInfo.schedulingDelay +
        ",processing Delay" + batchInfo.processingDelay

      setMap(imap)
   }

    if (totalRecords >= recordLimit) {
      if (hasStarted && !thread.isAlive) {
        //not receiving any data more, finish
        endTime = System.currentTimeMillis()
        val totalTime = (endTime - startTime).toDouble / 1000
        //This is weighted avg of every batch process time. The weight is records processed int the batch
        val avgLatency = totalDelay.toDouble / totalRecords
        if (avgLatency > batchSize.toDouble)
          println("WARNING:SPARK CLUSTER IN UNSTABLE STATE. TRY REDUCE INPUT SPEED")

        val avgLatencyAdjust = avgLatency + batchSize.toDouble
        val recordThroughput = recordLimit / totalTime

        val imap = getMap

        imap("Final Metric") = "Total Batch count = " + batchCount+
        ", Total processing delay = " + totalDelay + " ms "+
        ", Total Consumed time = " + totalTime + " s " +
        ", Avg latency/batchInterval = " + avgLatencyAdjust + " ms "+
        ", Avg records/sec = " + recordThroughput + " records/s "

        imap.foreach {case (key, value) => println(key + "-->" + value)}

        thread.start
      }
    } else if (!hasStarted) {
      startTime = batchCompleted.batchInfo.submissionTime
      hasStarted = true
    }

    if (hasStarted) {
      //      println("This delay:"+batchCompleted.batchInfo.processingDelay+"ms")
      batchCompleted.batchInfo.processingDelay match {
        case Some(value) => totalDelay += value * recordThisBatch
        case None => //Nothing
      }
      batchCount = batchCount + 1
    }
  }

}
