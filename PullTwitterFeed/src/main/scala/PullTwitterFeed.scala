/**
 * Created by sachin on 2/18/16.
 */


import com.google.gson.{Gson, GsonBuilder}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

object PullTwitterFeed extends App {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val numFilesPerPartition = 1;
  var receivedTweetCount: Long = 0;
  val tweetThreshold: Long = 1000;
  val outputDirectory = "/tmp/output";
  val receiverParallalism = 1;

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
  val gson: Gson = new GsonBuilder().create();

  val twitterAuth = new OAuthAuthorization(cb.build())
  val tweetStream =
    (1 to receiverParallalism).map { _ => TwitterUtils.createStream(ssc, Option(twitterAuth)).map(gson.toJson(_)) }
      //TwitterUtils.createStream(ssc, Option(twitterAuth)).map(status => status)}
      // TwitterUtils.createStream(ssc, Option(twitterAuth)).map(gson.toJson(_)) }
  val unionDStream = ssc.union(tweetStream)

  unionDStream.foreachRDD((rdd, time) => {
    val count = rdd.count()
    println("count" + count)
    if (count > 0) {
      print("rdd partition=" + rdd.partitions.length)
      val outputRDD = rdd.repartition(numFilesPerPartition)
      outputRDD.saveAsTextFile(
        outputDirectory + "/tweets/" + rdd.id)
      receivedTweetCount += count
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
