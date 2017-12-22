package net.kafka.example.first

import java.util.Properties

import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConverters._

object FirstKafkaConsumer extends App {

  val properties = new Properties()

  //kafka bootstrap server
  properties.setProperty("bootstrap.servers", "127.0.0.1:9092")
  properties.setProperty("key.deserializer", classOf[StringDeserializer].getName)
  properties.setProperty("value.deserializer", classOf[StringDeserializer].getName)
  properties.setProperty("group.id", "test")
  properties.setProperty("enable.auto.commit", "true")
//  properties.setProperty("auto.commit", "true") // or commit explicitly
  properties.setProperty("auto.commit.interval.ms", "1000")
  properties.setProperty("auto.offset.reset", "earliest")

  val kafkaConsumer: Consumer[String, String] = new KafkaConsumer(properties)
  kafkaConsumer.subscribe(List("first_topic").asJava)

  while(true) {
    val consumerRecords: ConsumerRecords[String, String] = kafkaConsumer.poll(1000)
    consumerRecords.asScala.foreach{cR => println(s"Partition: ${cR.partition()}. Offset: ${cR.offset()}. Key: ${cR.key()}. Value: ${cR.value()}")}
    kafkaConsumer.commitAsync()
  }

}
