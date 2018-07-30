package demo.twitter

import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object PopularTags {


  def main(args: Array[String]) {
    //setting properties for consuming from Twitter Streaming
    setTwitterProperties(args)

    //filters that are used to filter statuses in the Twitter API
    val filters = args.takeRight(args.length - 4)

    //setting the app name
    val sparkConf = new SparkConf().setAppName("TwitterPopularTags")

    // check Spark configuration for master URL, set it to local if not configured
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[*]")
    }

    // Instantiating the StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    // Creating the DStream from Twitter Streaming API
    val stream = TwitterUtils.createStream(ssc, None, filters)

    //creating a new of DStream[String]
    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    // reduceByKeyAndWindow -> available along with reduceByKey in PairDStreams
    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
      .map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(ascending = false))

    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
      .map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(ascending = false))


    // Print popular hashtags
    topCounts60.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })

    topCounts10.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })

    // Starting to stream
    ssc.start()

    // It keeps running indefinitely, until it is finished
    ssc.awaitTermination()
  }

  /**
    *
    * used to set the access token and secret keys to consume from twitter
    */
  private def setTwitterProperties(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println("Usage: TwitterPopularTags <consumer key> <consumer secret> " +
        "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }
    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)


    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
  }


}
