/**
 * Created by sachin on 12/10/15.
 */

package spark.benchmark

import benchmark.common.Utils
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}

import scala.collection.JavaConverters._

object TwitterStreaming {
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
    val batchSize = commonConfig.get("spark.batchtime") match {
      case n: Number => n.longValue()
      case other => throw new ClassCastException(other + " not a Number")
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
    .setMaster("local[*]")
    //  .set("spark.eventLog.dir","file:///tmp/spark-events")

    val ssc = new StreamingContext(sparkConf, Milliseconds(batchSize))

    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokerListString.toString())
    System.err.println(
      "Trying to connect to Kafka at " + kafkaBrokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    val lines = messages.map(_._2)
    val hashTags = lines.flatMap(status => status.split(" ").filter(_.startsWith("#")))
    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    topCounts60.foreachRDD((rdd, time) => {
      val formattedRdd = rdd.map({ case (count, tag) => (tag, count)})
      val count = formattedRdd.count()
      if (count > 0) {
        formattedRdd.saveAsTextFile(resultOutDir + "/" + rdd.id)
      }
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