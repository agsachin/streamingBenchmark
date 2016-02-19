/**
 * Created by sachin on 2/18/16.
 */


import java.util.concurrent.atomic.AtomicLong

import com.google.gson.{Gson, GsonBuilder}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

object test extends App {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val numFilesPerPartition =1;
  var receivedTweetCount:Long = 0;
  val tweetThreshold:Long = 1000;
  val outputDirectory = "/tmp/output";
  val runningCount:AtomicLong = new AtomicLong(0);

  val consumerKey = "taVpGWVGyRiFnQDsNwPwITRcH";
  val consumerSecret = "3UKCluIj3OuftNhfeXkIyoR0hd8fs2lhAV7Ki5OqGKMc84IpkJ";
  val accessToken = "145001241-n8zdD6gi71xWBC8v55eW7BIBQ2uizR21iUCzLGyu";
  val accessTokenSecret = "kJsg01s3g9aKGNmbbNgLvzSgCkX0QY3QUbV8XqeWqOAxl";

  val cb = new ConfigurationBuilder()
  cb.setDebugEnabled(true)
    .setOAuthConsumerKey(consumerKey)
    .setOAuthConsumerSecret(consumerSecret)
    .setOAuthAccessToken(accessToken)
    .setOAuthAccessTokenSecret(accessTokenSecret)

  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("TwitterPopularTags")
  val ssc = new StreamingContext(sparkConf, Seconds(2))
  val gson:Gson = new GsonBuilder().create();

  val twitterAuth = new OAuthAuthorization(cb.build())
  val tweetStream = TwitterUtils.createStream(ssc, Option(twitterAuth)).map(gson.toJson(_)).repartition(1)

  val tw = tweetStream.window(Seconds(6),Seconds(6))
  val twCount = tw.count()
  val tweetStreamCount=tweetStream.count()
  twCount.print()
  tweetStreamCount.print()
//  tw.foreachRDD((rdd, time) => {
//    println("tw:::::::::::::rdd_id="+rdd.id +"count="+rdd.count)
//    runningCount.getAndAdd(rdd.count());
//  })
//
//  tweetStream.foreachRDD((rdd, time) => {
//    println("tweetStream:::rdd_id="+rdd.id +"count="+rdd.count)
//  })

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
