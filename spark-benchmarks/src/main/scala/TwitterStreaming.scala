/**
 * Created by sachin on 12/10/15.
 */

package spark.benchmark

import benchmark.common.Utils
//import org.apache.log4j.{Level, Logger}

import org.apache.spark.{SparkConf,HashPartitioner}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime, StreamingQueryListener}
import org.apache.spark.sql.execution.streaming.{StreamExecution, MemorySink, Offset }

import scala.collection.JavaConverters._

object TwitterStreaming {

  private var imap: scala.collection.mutable.Map[String, Long] = _
  def getMap(): scala.collection.mutable.Map[String, Long] = synchronized {
    if (imap == null) imap = scala.collection.mutable.Map()
    imap
  }
  def setMap(imap: scala.collection.mutable.Map[String, Long]) = synchronized {
    this.imap = imap
  }

    //Logger.getLogger("org").setLevel(Level.OFF)
    //Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]) {
    val commonConfig = Utils.findAndReadConfigFile(args(0), true).asInstanceOf[java.util.Map[String, Any]];

    val serializer = commonConfig.get("kafka.serializer") match {
      case s: String => s
      case other => throw new ClassCastException(other + " not a String")
    }
    val kafkaBrokers = commonConfig.get("kafka.brokers").asInstanceOf[java.util.List[String]] match {
      case l: java.util.List[String] => l.asScala.toSeq
      case other => throw new ClassCastException(other + " not a List[String]")
    }
    val kafkaPort = commonConfig.get("kafka.port") match {
      case n: Number => n.toString()
      case other => throw new ClassCastException(other + " not a Number")
    }
    val topic = commonConfig.get("kafka.topic") match {
      case s: String => s
      case other => throw new ClassCastException(other + " not a String")
    }
    val resultOutDir = commonConfig.get("data.result.outputDirectory") match {
      case s: String => s
      case other => throw new ClassCastException(other + " not a String")
    }
    val batchSize = commonConfig.get("spark.performance.batchTime") match {
      case n: Number => n.longValue()
      case other => throw new ClassCastException(other + " not a Number")
    }
    val windowSize = commonConfig.get("spark.performance.windowSize") match {
      case n: Number => n.longValue()
      case other => throw new ClassCastException(other + " not a Number")
    }
    val checkPointPath = commonConfig.get("spark.performance.checkPointPath") match {
      case s: String => s
      case other => throw new ClassCastException(other + " not a String")
    }
    //  val kafkaHosts = "localhost:9092,localhost:9093,localhost:9094";
    // val topicsSet = topics.split(",").toSet
    val topicsSet = Set(topic)

    val brokerListString = new StringBuilder();

    for (host <- kafkaBrokers) {
      if (!brokerListString.isEmpty) {
        brokerListString.append(",")
      }
      brokerListString.append(host).append(":").append(kafkaPort)
    }
    // val brokerListString = joinHosts(kafkaBrokers, kafkaPort)


    val spark = SparkSession.builder()
      .appName("TwitterStreaming")
      .config("spark.eventLog.enabled","true")
      .config("spark.eventLog.dir","file:///tmp/spark-events")
      .getOrCreate()

    spark.conf.set("spark.sql.streaming.checkpointLocation", checkPointPath)
    spark.conf.set(org.apache.spark.sql.internal.SQLConf.STREAMING_SCHEMA_INFERENCE.key,"true")

    /* val listener = new LatencyListener(ssc,commonConfig)
    ssc.addStreamingListener(listener)
    ssc.sparkContext.addSparkListener(listener) */

    val reader = spark.readStream.format("json").load("hdfs://10.168.102.170:9000/user/streamdir")

    import spark.implicits._
    val hashtags = reader.select("text")
      .as[String]
      .flatMap{tweet => tweet.split(" ").filter(_.startsWith("#")) }

    hashtags.createOrReplaceTempView("hashTags")
    val hashtagsCount =
      spark.sql("select value, count(*) as count from hashTags group by value")

    val query = hashtagsCount.writeStream
      .outputMode(OutputMode.Complete)
      .format("console")
      .queryName("twitterstreaming")
      .start()


    query.awaitTermination()
  }

}
