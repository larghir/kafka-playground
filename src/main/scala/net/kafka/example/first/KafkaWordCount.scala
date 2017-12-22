package net.kafka.example.first

import com.github.benfradet.spark.kafka.writer._
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaWordCount extends App {

  val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[*]")
  val ssc = new StreamingContext(sparkConf, Seconds(15))
  val myBrokers = List("127.0.0.1:9092")
  val myTopic = Set("first_topic", "second_topic")

  import org.apache.kafka.clients.consumer.ConsumerConfig

  val consumerConfig =  Map[String, Object](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> myBrokers.mkString(","),
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
    ConsumerConfig.GROUP_ID_CONFIG -> "DemoConsumer",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
    ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG -> "1000",
    ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG -> "30000",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer")

  val producerConfig =  Map[String, Object](
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> myBrokers.mkString(","),
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer",
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer")


  val messages = KafkaUtils.createDirectStream[String, String](
    ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](myTopic, consumerConfig))

  val lines: DStream[String] = messages.map(_.value())
  val words: DStream[String] = lines.flatMap(_.split(" "))
  val wordCounts: DStream[(String, Int)] = words.map(w => (w, 1)).reduceByKey(_ + _)


  wordCounts.cache()
  //output the first ten elements of each generated RDD
  wordCounts.print()

  // publish words and counts back to the queue
  wordCounts.foreachRDD { rdd =>
    rdd.sortBy(el => el._2, false).writeToKafka(
      producerConfig,
      s => new ProducerRecord[String, String]("fourth_topic", s.toString)
    )
  }

  ssc.start()
  ssc.awaitTermination()
}
