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

class LatencyListener(ssc: StreamingContext,commonConfig : Map[String, Any] ) extends StreamingListener {

  var startTime=0L
  var endTime=0L
  //This delay is processDelay of every batch * record count in this batch
  var totalDelay=0L
  var hasStarted=false
  var batchCount=0
  var totalRecords=0L

  val thread: Thread = new Thread(new StopContextThread(ssc))

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

  val recordLimit=loaderThreads*recordLimitPerThread

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit={
    val batchInfo = batchCompleted.batchInfo
    val prevCount=totalRecords
    var recordThisBatch = batchInfo.numRecords

    if (!thread.isAlive) {
      totalRecords += recordThisBatch
      println("LatencyController:    batchCount so far: " + batchCount)
      println("LatencyController:    batchTime: " + batchInfo.batchTime)
      println("LatencyController:    submissionTime: " + batchInfo.submissionTime)
      println("LatencyController:    processingStartTime: " + batchInfo.processingStartTime)
      println("LatencyController:    processingEndTime: " + batchInfo.processingEndTime)
      println("LatencyController:    schedulingDelay: " + batchInfo.schedulingDelay)
      println("LatencyController:    processingDelay: " + batchInfo.processingDelay)
      println("LatencyController:    recordThisBatch: " + recordThisBatch)
      println("LatencyController:    total records: " + totalRecords)
    }

    if (totalRecords >= recordLimit) {
      if (hasStarted && !thread.isAlive) {
        //not receiving any data more, finish
        endTime = System.currentTimeMillis()
        val totalTime = (endTime-startTime).toDouble/1000
        //This is weighted avg of every batch process time. The weight is records processed int the batch
        val avgLatency = totalDelay.toDouble/totalRecords
        if (avgLatency > batchSize.toDouble)
          println("WARNING:SPARK CLUSTER IN UNSTABLE STATE. TRY REDUCE INPUT SPEED")

        val avgLatencyAdjust = avgLatency + batchSize.toDouble
        val recordThroughput = recordLimit / totalTime
        println("Batch count = " + batchCount)
        println("Total processing delay = " + totalDelay + " ms")
        println("Consumed time = " + totalTime + " s")
        println("Avg latency/batchInterval = " + avgLatencyAdjust + " ms")
        println("Avg records/sec = " + recordThroughput + " records/s")
        thread.start
      }
    } else if (!hasStarted) {
      startTime = batchCompleted.batchInfo.submissionTime
      hasStarted = true
    }

    if (hasStarted) {
//      println("This delay:"+batchCompleted.batchInfo.processingDelay+"ms")
      batchCompleted.batchInfo.processingDelay match {
        case Some(value) => totalDelay += value*recordThisBatch
        case None =>  //Nothing
      }
      batchCount = batchCount+1
    }
  }

}
