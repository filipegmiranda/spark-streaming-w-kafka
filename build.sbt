name := "spark-streaming-w-kafka"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.2.0"
libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.2.0"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.2.0"// % "provided"
libraryDependencies += "org.apache.avro" % "avro" % "1.8.2"
libraryDependencies += "com.twitter" %% "bijection-avro" % "0.9.6"