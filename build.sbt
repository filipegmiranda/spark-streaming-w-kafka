name := "spark-streaming-w-kafka"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.2.0"
//libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.0"


libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.2.0"

//libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.6.3"
//libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.3.0-SNAPSHOT"