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

import java.util

import org.apache.spark.scheduler._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler._
import scala.collection.mutable.Map

class StopContextThread(ssc: StreamingContext) extends Runnable {
  def run {
    ssc.stop(true, true)
  }
}

case class BatchData(batchInfo: BatchInfo, batchCount: Long, totalRecords: Long, recordThisBatch: Long) {

  override def toString(): String = {
    val kakfacompute = batchInfo.addl.kafka.end - batchInfo.addl.kafka.start
    val fdstreamcompute = batchInfo.addl.getfdstreamData().end - batchInfo.addl.getfdstreamData().start
    val windowCompute = batchInfo.addl.window.end - batchInfo.addl.window.start
    val remngGenJobTime = (batchInfo.addl.genEnd - batchInfo.addl.allocBlockEnd) - kakfacompute
    val dstreamCompute = (batchInfo.addl.kafka.end - batchInfo.addl.allocBlockEnd)   
    val rddCompute = (batchInfo.addl.genEnd - batchInfo.addl.kafka.end)   

    val tdstreamcompute = 0 //batchInfo.addl.getTdstreamData().end - batchInfo.addl.getTdstreamData().start
    val sdstreamcompute = batchInfo.addl.getSdstreamData().end - batchInfo.addl.getSdstreamData().start
    val fmdstreamcompute = batchInfo.addl.getFMdstreamData().end - batchInfo.addl.getFMdstreamData().start
    val mdstreamcompute0 = batchInfo.addl.getMdstreamData()(0).end - batchInfo.addl.getMdstreamData()(0).start
    val mdstreamcompute1 = batchInfo.addl.getMdstreamData()(1).end - batchInfo.addl.getMdstreamData()(1).start
    val mdstreamcompute2 = batchInfo.addl.getMdstreamData()(2).end - batchInfo.addl.getMdstreamData()(2).start

    "" + batchInfo.batchTime.milliseconds + "," + batchCount + "," + totalRecords +
      "," + recordThisBatch + "," + batchInfo.submissionTime + "," + batchInfo.processingStartTime + "," +
      batchInfo.processingEndTime + "," + batchInfo.schedulingDelay + "," + batchInfo.processingDelay + "," +
      (batchInfo.addl.actual.milliseconds - batchInfo.batchTime.milliseconds) + "," +
      (batchInfo.addl.queTime - batchInfo.addl.actual.milliseconds) + "," +
      (batchInfo.addl.genEnd - batchInfo.addl.allocBlockEnd) + "," +
      kakfacompute + "," + remngGenJobTime + "," + windowCompute + "," + dstreamCompute + "," + rddCompute + "," + batchInfo.addl.kafka.partInfo +
      "," + (batchInfo.addl.getEventEnd() - batchInfo.addl.genEnd) + "," + fdstreamcompute + "," +
      tdstreamcompute + "," + sdstreamcompute + "," + fmdstreamcompute + "," + 
      mdstreamcompute0 + "," + mdstreamcompute1 + "," + mdstreamcompute2

  }

  def getTaskdetails(stage:Stage,taskMap: Map[Long, Task],maxTask: Int): String = {
    try {
      var taskStr = for {
        index <- stage.tasks.sorted if taskMap.contains(index)
      } yield {
        val task = taskMap(index)
        /*(task.end - task.start) + " ," + task.start + " ," + task.end + " ," + task.deSerializeTime + " ," +
        task.executionTime + " ," + task.serializeTime + " ," + task.gcTime + " "*/
        (task.end - task.start) + " ," + task.deSerializeTime + " ," +
        task.executionTime + " ," + task.serializeTime + " ," + task.gcTime + " "
      }
      taskStr.mkString(",")
    } catch {
      case e:Exception => println(e.getMessage)
        println(taskMap)
        println("-------------------------")
        "  "
    }
  }

  def getJobdetails (job:Job, stageMap: Map[Int,Stage], taskMap: Map[Long, Task],maxTask: Int) : String = {
    var details = "," + (job.end - job.start) + "," //+ job.start + " ," + job.end + " ,"
    try {
      var stageStr = for {
        index <- job.stages.sorted if stageMap.contains(index)
      } yield {
          val stage = stageMap(index)
          var stagedetails = (stage.end - stage.start) + "," + stage.start + " ," + stage.end + " ,"
          stagedetails = stagedetails.concat(getTaskdetails(stage,taskMap,maxTask))
          if(stage.tasks.length < maxTask) {
            1 to maxTask - stage.tasks.length foreach(i => stagedetails = stagedetails.concat(",,,,,"))
          }
        stagedetails
      }
      details.concat(stageStr.mkString(","))
    } catch {
      case e:Exception => println(e.getMessage)
        println(stageMap)
        println("-------------------------")
        "  "
    }
  }

  def printStringForm(l:LatencyListener, key:String): Unit = {
    var batchInfoStr = this.toString()
    val jobId = l.batchTimejobIdMap.getOrElse(key,-1)

    //var job0details = getJobdetails(l.jobMap.getOrElse((jobId-1), new Job(-1)), l.stageMap, l.taskMap, l.maxTask)
    var job1details = getJobdetails(l.jobMap.getOrElse((jobId), new Job(-1)), l.stageMap, l.taskMap, l.maxTask)

    //println(batchInfoStr.concat(job0details).concat(job1details))
    println(batchInfoStr.concat(job1details))
  }

}

class Task(id:Long) {
  var start:Long = 0
  var end:Long = 0
  var executionTime:Long = 0
  var serializeTime:Long = 0
  var deSerializeTime:Long = 0
  var gcTime:Long = 0
  override def toString(): String = {
    start + " ," + end + " "
  }
}

class Stage(id:Int) {
  var start:Long = 0
  var end:Long = 0
  var tasks:Seq[Long] = Seq()
  override def toString(): String = {
    start + " ," + end + " "
  }
}

class Job(id:Int) {
  var end:Long = 0
  var start:Long = 0
  var stages:Seq[Int] = Seq()

  override def toString(): String = {
      start + " ," + end + " "
  }
}

object BatchData {
  def header(): String = {
    "batchTime,batchCount,totalRecords,recordThisBatch,submissionTime,processingStartTime," +
      "processingEndTime,schedulingDelay,processingDelay,actualDiff," +
      "QueueTime, genJobTime, kafkacomputeTime, remngGenJobTime, windowTime, DstreamEnd,rddCompute,partInfo,eventEndTime,fdstreamcompute," +
      "tdstreamcompute, sdstreamcompute, fmdstreamcompute,mdstreamcompute0,mdstreamcompute1,mdstreamcompute2"
  }
}

class LatencyListener(ssc: StreamingContext, commonConfig: util.Map[String, Any]) extends StreamingListener with SparkListener {

  var metricMap: scala.collection.mutable.Map[String, BatchData] = _
  var startTime = 0L
  var startTime1 = 0L
  var endTime = 0L
  var endTime1 = 0L
  var totalDelay = 0L
  var hasStarted = false
  var batchCount = 0
  var totalRecords = 0L
  val thread: Thread = new Thread(new StopContextThread(ssc))

  var batchTimejobIdMap: scala.collection.mutable.Map[String, Int] = scala.collection.mutable.Map()
  var jobMap: scala.collection.mutable.Map[Int, Job] = scala.collection.mutable.Map()
  var stageMap: scala.collection.mutable.Map[Int, Stage] = scala.collection.mutable.Map()
  var taskMap: scala.collection.mutable.Map[Long, Task] = scala.collection.mutable.Map()
  var maxTask: Int = 0


  def getMap(): scala.collection.mutable.Map[String, BatchData] = synchronized {
    if (metricMap == null) metricMap = scala.collection.mutable.Map()
    metricMap
  }

  def setMap(metricMap: scala.collection.mutable.Map[String, BatchData]) = synchronized {
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

  def getHeader() : String = {
    var tasksj0st0headers = ","
    var tasksj0st1headers = ","

    var tasksj1st0headers = ","
    var tasksj1st1headers = ","

    for(i <- 1 to maxTask) {
      tasksj0st0headers = tasksj0st0headers.concat( "J0st0task" + i + "Time," +
        //"J0st0task" + i + "start," + "J0st0task" + i + "end," +
        "J0st0task" + i + "DeSerTime," + "J0st0task" + i + "runTime," +
        "J0st0task" + i + "SerTime," + "J0st0task" + i + "gcTime,")
      tasksj0st1headers = tasksj0st1headers.concat( "J0st1task" + i + "Time," +
       //"J0st1task" + i + "start," + "J0st1task" + i + "end,"+
       "J0st1task" + i + "DeSerTime," + "J0st1task" + i + "runTime," +
       "J0st1task" + i + "SerTime," + "J0st1task" + i + "gcTime,")
      tasksj1st0headers = tasksj1st0headers.concat( "J1st0task" + i + "Time," +
        //"J1st0task" + i + "start," + "J1st0task" + i + "end,"+
        "J1st0task" + i + "DeSerTime," + "J1st0task" + i + "runTime," +
        "J1st0task" + i + "SerTime," + "J1st0task" + i + "gcTime,")
      tasksj1st1headers = tasksj1st1headers.concat( "J1st1task" + i + "Time," +
        //"J1st1task" + i + "start," + "J1st1task" + i + "end,"+
        "J1st1task" + i + "DeSerTime," + "J1st1task" + i + "runTime," +
        "J1st1task" + i + "SerTime," + "J1st1task" + i + "gcTime,")
    }

    var header = BatchData.header()

    //header = header.concat(",J0Time,J0Start,J0End,J0st0Time,J0st0start,Jst0end")
    //header = header.concat(",J0Time,J0st0Time")
    header = header.concat(",J0Time,J0st0Time,J0st0start,Jst0end")

    header = header.concat(tasksj0st0headers)
    header = header.concat("J0St1Time,J0st1start,J0st1end")
    //header = header.concat("J0St1Time")
    header = header.concat(tasksj0st1headers)

    //header = header.concat("J1Time,J1Start,J1End,J1st0Time,J1st0start,J1st0end")
    //header = header.concat("J1Time,J1st0Time")
    header = header.concat("J1Time,J1st0Time,J1st0start,J1st0end")
    header = header.concat(tasksj1st0headers)
    header = header.concat("J1st1Time,J1st1start,J1st1end")
    //header = header.concat("J1st1Time")
    header = header.concat(tasksj1st1headers)

    header
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    val batchInfo = batchCompleted.batchInfo
    val prevCount = totalRecords
    var recordThisBatch = batchInfo.numRecords

    if (!thread.isAlive) {
      totalRecords += recordThisBatch
      val imap = getMap
      imap(batchInfo.batchTime.toString()) = BatchData(batchInfo,
        batchCount, totalRecords, recordThisBatch)

        /*"batchTime," + batchInfo.batchTime +
        ", batch Count so far," + batchCount +
        ", total Records so far," + totalRecords +
        ", record This Batch," + recordThisBatch +
        ", submission Time," + batchInfo.submissionTime +
        ", processing Start Time," + batchInfo.processingStartTime.getOrElse(0L) +
        ", processing End Time," + batchInfo.processingEndTime.getOrElse(0L) +
        ", scheduling Delay," + batchInfo.schedulingDelay.getOrElse(0L) +
        ", processing Delay," + batchInfo.processingDelay.getOrElse(0L) */

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

        /*imap("Final Metric") = " Total Batch count," + batchCount +
          ", startTime based on submissionTime,"+startTime +
        ", startTime based on System,"+startTime1 +
          ", endTime based on System,"+endTime +
        ", endTime based on processingEndTime,"+endTime1 +
        ", Total Records,"+totalRecords+
       // ", Total processing delay = " + totalDelay + " ms "+
        ", Total Consumed time in sec," + totalTime +
        ", Avg latency/batchInterval in ms," + avgLatencyAdjust +
        ", Avg records/sec," + recordThroughput +
        ", WARNING,"+ warning */

        println("")
        //println(BatchData.header())

        var header = getHeader()

        println(header)

        //imap.foreach {case (key, value) => println(key + "-->" + value)}
        imap.foreach {case (key, value) => value.printStringForm(this,key)}

        println(" Total Batch count," + batchCount +
         ", startTime based on submissionTime,"+startTime +
       ", startTime based on System,"+startTime1 +
         ", endTime based on System,"+endTime +
       ", endTime based on processingEndTime,"+endTime1 +
       ", Total Records,"+totalRecords+
      // ", Total processing delay = " + totalDelay + " ms "+
       ", Total Consumed time in sec," + totalTime +
       ", Avg latency/batchInterval in ms," + avgLatencyAdjust +
       ", Avg records/sec," + recordThroughput +
       ", WARNING,"+ warning )

        thread.start
      }
    } else if (!hasStarted) {
      if (batchInfo.numRecords>0) {
        startTime = batchCompleted.batchInfo.submissionTime
        startTime1 =  System.currentTimeMillis()
        hasStarted = true
      }
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

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) : Unit = {
    val stage = stageMap(stageCompleted.stageInfo.stageId)
    stage.start = stageCompleted.stageInfo.submissionTime.getOrElse(0)
    stage.end = stageCompleted.stageInfo.completionTime.getOrElse(0)
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) : Unit = {
    val stage = new Stage( stageSubmitted.stageInfo.stageId )
    //println("pd" + stageSubmitted.stageInfo.stageId + "pd")
    stageMap(stageSubmitted.stageInfo.stageId) = stage
    if(stageSubmitted.stageInfo.numTasks > maxTask) {
      maxTask = stageSubmitted.stageInfo.numTasks
    }
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart) : Unit = {
    val task = new Task( taskStart.taskInfo.taskId )
    taskMap(taskStart.taskInfo.taskId) = task
    stageMap(taskStart.stageId).tasks = (stageMap(taskStart.stageId).tasks :+ taskStart.taskInfo.taskId).asInstanceOf[Seq[Long]]
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) : Unit = {
    val task = taskMap(taskEnd.taskInfo.taskId)
    task.start = taskEnd.taskInfo.launchTime
    task.end = taskEnd.taskInfo.finishTime
    task.deSerializeTime = taskEnd.taskMetrics.executorDeserializeTime
    task.serializeTime = taskEnd.taskMetrics.resultSerializationTime
    task.executionTime = taskEnd.taskMetrics.executorRunTime
    task.gcTime = taskEnd.taskMetrics.jvmGCTime
  }


  override  def onJobStart(jobStart: SparkListenerJobStart) : Unit = {
    val job = new Job(jobStart.jobId)
    job.start = jobStart.time
    job.stages = jobStart.stageIds
    jobMap(jobStart.jobId) = job
    val batchTime = jobStart.properties.get("spark.streaming.internal.batchTime")
    if(batchTime != null) {
      batchTimejobIdMap( batchTime.toString + " ms" ) = jobStart.jobId
    }
  }

  override  def onJobEnd(jobEnd: SparkListenerJobEnd) : Unit = {
    val job = jobMap(jobEnd.jobId)
    job.end = jobEnd.time
  }
}
