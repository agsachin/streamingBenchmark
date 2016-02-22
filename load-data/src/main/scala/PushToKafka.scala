package benchmark.common

import java.util.Properties

import com.google.gson.JsonParser
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

import scala.collection.JavaConverters._
import scala.io.Source._

/**
 * Created by sachin on 2/18/16.
 */

object PushToKafka{
  def main(args: Array[String]) {
    val commonConfig = Utils.findAndReadConfigFile(args(0), true).asInstanceOf[java.util.Map[String, Any]];

//    val brokerListString = commonConfig.get("kafka.brokerListString") match {
//      case s: String => s
//      case other => throw new ClassCastException(other + " not a String")
//    }
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
      case n: Number => n.toString()
      case other => throw new ClassCastException(other + " not a Number")
    }

    // Create direct kafka stream with brokers and topics
    val topicsSet = Set(topic)
    val brokerListString = joinHosts(kafkaBrokers, kafkaPort)
    //    val BROKER_LIST: String = "localhost:9092,localhost:9093,localhost:9094";
    //    val SERIALIZER: String = "kafka.serializer.StringEncoder";
    //    val REQUIRED_ACKS: String = "1";
    //    val KAFKA_TOPIC: String = "test1";


    /** Producer properties **/
    var props: Properties = new Properties()
    props.put("metadata.broker.list", brokerListString)
    props.put("auto.offset.reset" , "smallest")
    props.put("serializer.class", serializer)
    props.put("request.required.acks", requiredAcks)

    val config: ProducerConfig = new ProducerConfig(props)
    val producer: Producer[String, String] = new Producer[String, String](config)

    def send(text: String) {
      val data: KeyedMessage[String, String] = new KeyedMessage[String, String](topic, text);
      println(text)
      producer.send(data);
    }

    fromFile(inputDirectory).getLines.foreach(line => {
      val text = new JsonParser().parse(line).getAsJsonObject().get("text");
      //println(text.getAsString)
      Thread.sleep(1)
      send(text.getAsString)
    })
  }

  def joinHosts(hosts: Seq[String], port: String): String = {
    val joined = new StringBuilder();

    for (host <- hosts) {
      if (!joined.isEmpty) {
        joined.append(",")
      }
      joined.append(host).append(":").append(port)

    }
    return joined.toString()
  }
}
