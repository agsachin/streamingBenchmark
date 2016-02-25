///**
// * Created by sachin on 2/24/16.
// */
//
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.connectors.twitter.TwitterSource
//import org.apache.flink.streaming.connectors.json.JSONParseFlatMap
//import org.apache.flink.util.Collector
//import scala.util.Try
//
//object TwitterStream {
//  def main(args: Array[String]) {
//
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//
//    val streamSource = env.addSource(new TwitterSource("/tmp/twitter/twitter-auth.properties"))
//
//    val tweets = streamSource
//      .flatMap(new JsonLanguageParsers).name("Filtering languages")
//      .filter(_.nonEmpty)
////      .map((_,1))
////      .keyBy(0)
////      .sum(1)
//
//
//    tweets.print
//
//
//    env.execute("Twitter Stream")
//  }
//}
//
//class JsonCityParsers extends JSONParseFlatMap[String,String] {
//
//  override def flatMap(value: String, out: Collector[String]): Unit = {
//    if (Try(getString(value, "lang")).getOrElse("") == "en") {
//      out.collect(Try(getString(value, "place.name")).getOrElse(""))
//    }
//  }
//}
//
//class JsonLanguageParsers extends JSONParseFlatMap[String,String] {
//
//  override def flatMap(value: String, out: Collector[String]): Unit = {
//    out.collect(Try(getString(value, "lang")).getOrElse(""))
//  }
//}