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
  var startTime1 = 0L
  var endTime = 0L
  var endTime1 = 0L
  var totalDelay = 0L
  var totalJobSetCreationDelay = 0L
  var totalProcessingDelay = 0L
  var totalProcessingStartDelay = 0L
  var totalSubimissionDelay = 0L
  var hasStarted = false
  var batchCount = 0
  var totalRecords = 0L
  val thread: Thread = new Thread(new StopContextThread(ssc))


  def getMap(): scala.collection.mutable.Map[String, Object] = synchronized {
    if (metricMap == null) metricMap = scala.collection.mutable.Map()
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
      imap(batchInfo.batchTime.toString()) = batchInfo.batchTime +
        "," + batchCount +
        "," + totalRecords +
        "," + recordThisBatch +
        "," + batchInfo.batchJobSetCreationDelay +
        "," + batchInfo.processingStartTime.zip(Option(batchInfo.submissionTime)).map(x => x._1 - x._2).head +
        "," + Option(batchInfo.submissionTime).zip(batchInfo.jobSetCreationStartTime).map(x => x._1 - x._2).head +
        "," + batchInfo.submissionTime +
        "," + batchInfo.processingStartTime.getOrElse(0L) +
        "," + batchInfo.processingEndTime.getOrElse(0L) +
        "," + batchInfo.schedulingDelay.getOrElse(0L) +
        "," + batchInfo.processingDelay.getOrElse(0L)

      setMap(imap)
   }

    if (totalRecords >= recordLimit) {
      if (hasStarted && !thread.isAlive) {
        //not receiving any data more, finish
        endTime = System.currentTimeMillis()
        endTime1 = batchInfo.processingEndTime.getOrElse(0L)
        var warning=""
        val totalTime = (endTime - startTime).toDouble / 1000
        //This is weighted avg of every batch process time. The weight is records processed int the batch
        val avgLatency = totalDelay.toDouble / totalRecords
        if (avgLatency > batchSize.toDouble)
          warning="WARNING:SPARK CLUSTER IN UNSTABLE STATE. TRY REDUCE INPUT SPEED"

        val avgLatencyAdjust = avgLatency + batchSize.toDouble
        val recordThroughput = totalRecords / totalTime

        val imap = getMap

        imap("Final Metric") = " Total Batch count," + batchCount+
          ", startTime based on submissionTime,"+startTime +
        ", startTime based on System,"+startTime1 +
          ", endTime based on System,"+endTime +
        ", endTime based on processingEndTime,"+endTime1 +
        ", Total Records,"+totalRecords+
        ", Avg JobSetCreationDelay delay = " + totalJobSetCreationDelay/batchCount + " ms "+
        ", Avg ProcessingDelay delay = " + totalProcessingDelay/batchCount + " ms "+
        ", Avg ProcessingMinusSubmission delay = " + totalProcessingStartDelay/batchCount + " ms "+
        ", Avg SubmissionMinusCreate delay = " + totalSubimissionDelay/batchCount + " ms "+
        ", Total Consumed time in sec," + totalTime +
        ", Avg latency/batchInterval in ms," + avgLatencyAdjust +
        ", Avg records/sec," + recordThroughput +
        ", WARNING,"+ warning

        imap.foreach {case (key, value) => println(key + "-->" + value)}

        thread.start
      }
    } else if (!hasStarted) {
      if (batchInfo.numRecords>0) {
        startTime = batchCompleted.batchInfo.submissionTime
        startTime1 =  System.currentTimeMillis()
        hasStarted = true
        val imap = getMap
        imap("headers") = "batchTime, batch Count so far, total Records so far, record This Batch, jobGenerateTime, processing-submission, submission-create, submission Time, processing Start Time, processing End Time, scheduling Delay, processing Delay "
        setMap(imap)
      }
    }

    if (hasStarted) {
      //      println("This delay:"+batchCompleted.batchInfo.processingDelay+"ms")
      val batchinfo = batchCompleted.batchInfo
      batchinfo.processingDelay match {
        case Some(value) => totalDelay += value * recordThisBatch
        case None => //Nothing
      }
      batchInfo.batchJobSetCreationDelay match {
        case Some(value) => totalJobSetCreationDelay += value
        case None => //Nothing
      }
      batchInfo.processingDelay match {
        case Some(value) => totalProcessingDelay += value
        case None => //Nothing
      }
      batchInfo.processingStartTime.zip(Option(batchInfo.submissionTime)).map(x => x._1 - x._2).headOption match {
        case Some(value) => totalProcessingStartDelay += value
        case None => //Nothing
      }
      Option(batchInfo.submissionTime).zip(batchInfo.jobSetCreationEndTime).map(x => x._1 - x._2).headOption match {
        case Some(value) => totalSubimissionDelay += value
        case None => //Nothing
      }

      batchCount = batchCount + 1
    }
  }

}
