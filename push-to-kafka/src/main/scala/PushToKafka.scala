package benchmark.common.kafkaPush

import java.util.Properties

import com.google.gson.JsonParser
import benchmark.common.Utils
import scala.collection.JavaConverters._
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

import scala.io.Source._

/**
 * Created by sachin on 2/18/16.
 */

object PushToKafka{
  def main(args: Array[String]) {
    val commonConfig = Utils.findAndReadConfigFile(args(0), true).asInstanceOf[java.util.Map[String, Any]];

    val serializer = commonConfig.get("kafka.serializer") match {
      case s: String => s
      case other => throw new ClassCastException(other + " not a String")
    }
    val requiredAcks = commonConfig.get("kafka.requiredAcks") match {
      case s: String => s
      case other => throw new ClassCastException(other + " not a String")
    }
    val topic = commonConfig.get("kafka.topic") match {
      case s: String => s
      case other => throw new ClassCastException(other + " not a String")
    }
    val inputDirectory = commonConfig.get("data.inputDirectory") match {
      case s: String => s
      case other => throw new ClassCastException(other + " not a String")
    }

    val kafkaBrokers = commonConfig.get("kafka.brokers").asInstanceOf[java.util.List[String]] match {
      case l: java.util.List[String] => l.asScala.toSeq
      case other => throw new ClassCastException(other + " not a List[String]")
    }
    val kafkaPort = commonConfig.get("kafka.port") match {
      case n: Number => n.longValue()
      case other => throw new ClassCastException(other + " not a Number")
    }

    // Create direct kafka stream with brokers and topics

    //        val brokerListString: String = "localhost:9092,localhost:9093,localhost:9094";
    //        val serializer: String = "kafka.serializer.StringEncoder";
    //        val requiredAcks: String = "1";
    //        val topic: String = "test1";
    //        val inputDirectory="/tmp/data/event";

    val brokerListString = new StringBuilder();

    for (host <- kafkaBrokers) {
      if (!brokerListString.isEmpty) {
        brokerListString.append(",")
      }
      brokerListString.append(host).append(":").append(kafkaPort)
    }

    // println("all props="+" brokerListString.toString()"+ brokerListString.toString()+"serializer"+serializer+"requiredAcks"+requiredAcks)
    /** Producer properties **/
    var props: Properties = new Properties()
    props.put("metadata.broker.list", brokerListString.toString())
    props.put("auto.offset.reset", "smallest")
    props.put("serializer.class", serializer)
    props.put("request.required.acks", requiredAcks)

    val config: ProducerConfig = new ProducerConfig(props)
    val producer: Producer[String, String] = new Producer[String, String](config)

    def send(text: String) {
      val data: KeyedMessage[String, String] = new KeyedMessage[String, String](topic, text)
      // println(text)
      producer.send(data);
    }
    while (true) {
    fromFile(inputDirectory).getLines.foreach(line => {
      val text = new JsonParser().parse(line).getAsJsonObject().get("text")
      //println(text.getAsString)
      // Thread.sleep(1)
      send(text.getAsString)
    })
  }
    producer.close()
  }
}
