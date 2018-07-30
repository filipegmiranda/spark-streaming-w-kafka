package demo.kafka

import java.util.Properties

import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import kafka.serializer.DefaultDecoder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, BytesDeserializer, StringDeserializer}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}



object KafkaSparkStreaming {

  val parser = new Schema.Parser()
  val schema = parser.parse(KafkaProducer.userSchema)
  val recordInjection = GenericAvroCodecs.toBinary[GenericRecord](schema)

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("KafkaStreamingExample")
      .setMaster("local[*]")
    val streamingContext = new StreamingContext(sparkConf, Seconds(10))
    val topics = Array(KafkaProducer.topicUsers)

    val stream = KafkaUtils.createDirectStream[String, Array[Byte]](
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, Array[Byte]](topics, kafkaParams)
    )

    stream.foreachRDD { rdd =>
      rdd.foreach { message =>
        val genericRecord: GenericRecord = recordInjection.invert(message.value()).get
        println(s"the name is: ${genericRecord.get("name")}")
      }
    }
    streamingContext.start()
    streamingContext.awaitTermination()
    produceSampleMessages(10000)
  }


  private val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[ByteArrayDeserializer],
    "group.id" -> "group_example",
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  def produceSampleMessages(nr: Int): Unit = {
    val t = new Thread( new Runnable {
      override def run(): Unit = {
        KafkaProducer.produceMessages(nr)
      }
    })
    t.start()
  }

}


object KafkaProducer {



  val userSchema = "{" +
    "\"type\":\"record\"," +
    "\"name\":\"example_record\"," +
    "\"fields\":[" +
    "  { \"name\":\"name\", \"type\":\"string\" }," +
    "  { \"name\":\"last_name\", \"type\":\"string\" }," +
    "  { \"name\":\"nr\", \"type\":\"int\" }" +
    "]}"

  val topicUsers = "topic_users"

  import org.apache.avro.generic.GenericData
  import org.apache.avro.generic.GenericRecord
  import org.apache.kafka.clients.producer.KafkaProducer
  import org.apache.kafka.clients.producer.ProducerRecord


  def main(args: Array[String]): Unit = {
    val nr = args(0).toInt
    produceMessages(nr)
  }

  def produceMessages(nr: Int): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    val parser = new Schema.Parser()
    val schema = parser.parse(userSchema)
    val recordInjection: Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schema)
    val producer = new KafkaProducer[String, Array[Byte]](props)
    var i = 0
    while (i < nr) {
      val avroRecord: GenericData.Record = new GenericData.Record(schema)
      avroRecord.put("name", "name-" + i)
      avroRecord.put("last_name", "last name-" + i)
      avroRecord.put("nr", i)
      val bytes = recordInjection.apply(avroRecord)
      val record = new ProducerRecord[String, Array[Byte]](topicUsers, bytes)
      producer.send(record)
      //Thread.sleep(250)
      i += 1;
    }
    Thread.sleep(15000)
    producer.close()
  }

}