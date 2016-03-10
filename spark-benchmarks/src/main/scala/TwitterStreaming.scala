/**
 * Created by sachin on 12/10/15.
 */

package spark.benchmark

import benchmark.common.Utils
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

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

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

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


    val sparkConf = new SparkConf()
      .setAppName("TwitterStreaming")
      .set("spark.eventLog.enabled","true")
    //.setMaster("local[*]")
    //.set("spark.eventLog.dir","file:///tmp/spark-events")

    val ssc = new StreamingContext(sparkConf, Milliseconds(batchSize))
    //ssc.checkpoint(checkPointPath)

    val listener = new LatencyListener(ssc,commonConfig)
    ssc.addStreamingListener(listener)

    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokerListString.toString())
    System.err.println(
      "Trying to connect to Kafka at " + brokerListString.toString())
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    val lines = messages.map(_._2)
    val hashTags = lines.flatMap(status => status.split(" ").filter(_.startsWith("#")))
    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Milliseconds(windowSize))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    topCounts60.foreachRDD(rdd=> {val topList = rdd.take(10)
      val imap = getMap
      topList.foreach{case (count, tag) =>
        imap(tag) = if (imap.contains(tag)) imap(tag) + count else count
      }
      setMap(imap)
    })

    sys.ShutdownHookThread {
      println("Gracefully stopping Spark Streaming Application")
      ssc.stop(true, true)
      println("Application stopped")
    }

    //    topCounts60.foreachRDD(rdd => {
    //      val topList = rdd.take(10)
    //      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
    //    })

    ssc.start()
    ssc.awaitTermination()
  }

}