name := "PullTwitterFeed"

version := "1.0"

scalaVersion := "2.11.7"


libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "4.0.4"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.6.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.6.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.6.0"

libraryDependencies += "com.google.code.gson" % "gson" % "2.6.1"
