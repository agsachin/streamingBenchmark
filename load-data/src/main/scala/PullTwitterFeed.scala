package benchmark.common.twitterPull

import java.util.logging.{Level, Logger}

import benchmark.common._
import com.google.gson.GsonBuilder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}


/**
 * Created by sachin on 2/18/16.
 */



object PullTwitterFeed {
  def main(args: Array[String]) {
    val commonConfig = Utils.findAndReadConfigFile(args(0), true).asInstanceOf[java.util.Map[String, Any]];

    val consumerKey = commonConfig.get("twitter.consumerKey") match {
      case s: String => s
      case other => throw new ClassCastException(other + " not a String")
    }
    val consumerSecret = commonConfig.get("twitter.consumerSecret") match {
      case s: String => s
      case other => throw new ClassCastException(other + " not a String")
    }
    val accessToken = commonConfig.get("twitter.accessToken") match {
      case s: String => s
      case other => throw new ClassCastException(other + " not a String")
    }
    val accessTokenSecret = commonConfig.get("twitter.accessTokenSecret") match {
      case s: String => s
      case other => throw new ClassCastException(other + " not a String")
    }
    val outputDirectory = commonConfig.get("data.twitter.outputDirectory") match {
      case s: String => s
      case other => throw new ClassCastException(other + " not a String")
    }
    val numFilesPerPartition = commonConfig.get("data.twitter.write.numFilesPerPartition") match {
      case n: Number => n.intValue()
      case other => throw new ClassCastException(other + " not a Number")
    }
    var receivedTweetCount:Long = 0 ;
    val tweetThreshold = commonConfig.get("data.twitter.tweet.threshold") match {
      case n: Number => n.longValue()
      case other => throw new ClassCastException(other + " not a Number")
    }
    val receiverParallalism = commonConfig.get("data.twitter.receiver.parallalism") match {
      case n: Number => n.intValue()
      case other => throw new ClassCastException(other + " not a Number")
    }
    val batchSize = commonConfig.get("spark.twitterPull.batchTime") match {
      case n: Number => n.longValue()
      case other => throw new ClassCastException(other + " not a Number")
    }
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

//    val numFilesPerPartition = 1;
//    var receivedTweetCount: Long = 0;
//    val tweetThreshold: Long = 1000;
//    val outputDirectory = "/tmp/output";
//    val receiverParallalism = 1;

//    val consumerKey = "taVpGWVGyRiFnQDsNwPwITRcH";
//    val consumerSecret = "3UKCluIj3OuftNhfeXkIyoR0hd8fs2lhAV7Ki5OqGKMc84IpkJ";
//    val accessToken = "145001241-n8zdD6gi71xWBC8v55eW7BIBQ2uizR21iUCzLGyu";
//    val accessTokenSecret = "kJsg01s3g9aKGNmbbNgLvzSgCkX0QY3QUbV8XqeWqOAxl";

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf()
      .setAppName("PullTwitterFeed")
      //.setMaster("local[*]")
      .set("spark.eventLog.enabled","true")

    val ssc = new StreamingContext(sparkConf, Milliseconds(batchSize))

   // val gson: Gson = new GsonBuilder().create();

    val tweetStream =
      (1 to receiverParallalism).map { _ => TwitterUtils.createStream(ssc, None).map(new GsonBuilder().create().toJson(_)) }
    //TwitterUtils.createStream(ssc, Option(twitterAuth)).map(status => status)}
    //TwitterUtils.createStream(ssc, Option(twitterAuth)).map(gson.toJson(_)) }
    val unionDStream = ssc.union(tweetStream)

    unionDStream.foreachRDD((rdd, time) => {
      val count = rdd.count()
      println("count" + count)
      if (count > 0) {
        print("rdd partition=" + rdd.partitions.length)
        val outputRDD = rdd.repartition(numFilesPerPartition)
        outputRDD.saveAsTextFile(
          outputDirectory + "/" + rdd.id)
        receivedTweetCount += count
        if (tweetThreshold == 0){
          receivedTweetCount = 0;
        }
        if (receivedTweetCount > tweetThreshold) {
          //sparkConf.set("timeToStop","true");
          System.exit(0)
        }
      }
    })

    sys.ShutdownHookThread {
      println("Gracefully stopping Spark Streaming Application")
      ssc.stop(true, true)
      println("Application stopped")
    }

    //tweetStream.repartition(3).saveAsTextFiles(outputDirectory + "/stream/tweets")

    ssc.start()
    //ssc.stop(sparkConf.getBoolean("timeToStop",false))
    ssc.awaitTermination()
  }
}
