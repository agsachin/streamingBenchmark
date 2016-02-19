import java.util.Properties

import com.google.gson.JsonParser

/**
 * Created by sachin on 2/18/16.
 */


import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}

import scala.io.Source._

object PushToKafka extends App{
  val BROKER_LIST: String = "localhost:9092,localhost:9093,localhost:9094";
  val SERIALIZER: String = "kafka.serializer.StringEncoder";
  val REQUIRED_ACKS: String = "1";
  val KAFKA_TOPIC: String = "test1";

  /** Producer properties **/
  var props: Properties = new Properties()
  props.put("metadata.broker.list", BROKER_LIST)
  props.put("serializer.class", SERIALIZER)
  props.put("request.required.acks", REQUIRED_ACKS)

  val config: ProducerConfig = new ProducerConfig(props)
  val producer: Producer[String, String] = new Producer[String, String](config)

  def send(text: String) {
    val data: KeyedMessage[String, String] = new KeyedMessage[String, String](KAFKA_TOPIC, text);
    println(text)
    producer.send(data);
  }

  fromFile("/tmp/output/tweets/17/part-00000").getLines.foreach(line => {
    val text = new JsonParser().parse(line).getAsJsonObject().get("text");
    //println(text.getAsString)
    send(text.getAsString)
  })

}
