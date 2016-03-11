package benchmark.common.kafkaPush


import java.util.Properties

import benchmark.common.Utils
import com.google.gson.JsonParser
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

import scala.collection.JavaConverters._
import scala.io.Source

/**
 * Created by sachin on 2/18/16.
 */

object PushToKafka {
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
    val inputFile = commonConfig.get("data.kafka.inputFile") match {
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
    val recordLimitPerThread = commonConfig.get("data.kafka.Loader.thread.recordLimit") match {
      case n: Number => n.longValue()
      case other => throw new ClassCastException(other + " not a Number")
    }
    val loaderThreads = commonConfig.get("data.kafka.Loader.thread") match {
      case n: Number => n.intValue()
      case other => throw new ClassCastException(other + " not a Number")
    }
    val kafkaPartitions = commonConfig.get("kafka.partitions") match {
      case n: Number => n.intValue()
      case other => throw new ClassCastException(other + " not a Number")
    }

    val brokerListString = new StringBuilder();

    for (host <- kafkaBrokers) {
      if (!brokerListString.isEmpty) {
        brokerListString.append(",")
      }
      brokerListString.append(host).append(":").append(kafkaPort)
    }

    var props: Properties = new Properties()
    props.put("metadata.broker.list", brokerListString.toString())
    props.put("auto.offset.reset", "smallest")
    props.put("serializer.class", serializer)
    props.put("request.required.acks", requiredAcks)

    val config: ProducerConfig = new ProducerConfig(props)
    //    val producer: Producer[String, String] = new Producer[String, String](config)
    //    val r = scala.util.Random
    var thread: Array[Thread] = new Array[Thread](loaderThreads + 1)

    for (i <- 1 to loaderThreads) {
      thread(i) = new Thread {
        override def run {

          val threadId: Long = Thread.currentThread().getId();
          val producer: Producer[String, String] = new Producer[String, String](config)
          val r = scala.util.Random
          var count: Long = 0
          val bufferedSource = Source.fromFile(inputFile)
          //val line = bufferedSource.getLines
         // val jsonParser=new JsonParser();

          val printThread = new Thread() {
            var prevCount:Long=0
            override def run {
              while (thread(i).isAlive) {
                println("threadId:" + threadId + "count:" + count+"event in this run = "+(count-prevCount))
                prevCount = count
                Thread.sleep(1000)
              }
            }
          }
          printThread.start()
          val startTime = java.lang.System.currentTimeMillis()
          println("threadId:"+threadId+", StartTime:"+ startTime)
//            for (i<-1L to recordLimitPerThread){
//              val text = jsonParser.parse(line.next()).getAsJsonObject().get("text")
//              val id = r.nextInt(kafkaPartitions)
//              val data: KeyedMessage[String, String] = new KeyedMessage[String, String](topic, id.toString, text.toString)
//              producer.send(data)
//              count=i;
//            }
          bufferedSource.getLines.foreach(line => {
            if (count <= recordLimitPerThread+10000) {
              val text = new JsonParser().parse(line).getAsJsonObject().get("text")
              val id = r.nextInt(kafkaPartitions)
              val data: KeyedMessage[String, String] = new KeyedMessage[String, String](topic, id.toString, id.toString, text.toString)
            //println(id)
              producer.send(data)
            }else{
              bufferedSource.close
            }
            count += 1
          })
          val endTime=java.lang.System.currentTimeMillis()
          println("EndTime:"+endTime)
          println("Time taken by job:"+(endTime-startTime))
          producer.close()
        }
      }

      thread(i).start
    }
    for (i <- 1 to loaderThreads) {
      thread(i).join();
    }
  }
}


//          while (flag) {
//            fromFile(inputDirectory).getLines.foreach(line => {
//              val text = new JsonParser().parse(line).getAsJsonObject().get("text")
//              val id = r.nextInt(kafkaPartitions)
//              val data: KeyedMessage[String, String] = new KeyedMessage[String, String](topic, id.toString, text.toString)
//
//              count += 1
//              if (count <= recordLimitPerThread){
//                producer.send(data)
//              }else{
//                flag=false
//              }
//            })
//          }


//val printThread = new Thread {
//  override def run: Unit = {
//    while (true) {
//      var prevOffSet:Long=0;
//      val threadId: Long = Thread.currentThread().getId();
//      println("thread=" + threadId)
//      for (i <- 1 to kafkaPartitions) {
//        val offSet: Long = SimpleExample.offSet(topic, i, kafkaBrokers.asJava, kafkaPort.toInt)
//        if (offSet == 0) {
//          System.out.println("Can't find offSet for Topic and Partition. Exiting")
//          return
//        }
//        println("partition=" + i + " and offset=" + offSet +"and record sent in this one are "+(prevOffSet - offSet))
//        prevOffSet=offSet;
//
//        Thread.sleep(1000)
//      }
//    }
//  }
//}
