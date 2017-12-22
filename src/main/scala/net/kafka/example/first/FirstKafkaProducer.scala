package net.kafka.example.first

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object FirstKafkaProducer extends App {

  val properties = new Properties()

  properties.setProperty("bootstrap.servers", "127.0.0.1:9092")
  properties.setProperty("key.serializer", classOf[StringSerializer].getName)
  properties.setProperty("value.serializer", classOf[StringSerializer].getName)
  properties.setProperty("acks", "1")
  properties.setProperty("retries", "3")
  properties.setProperty("linger.ms", "1")

  val producer: Producer[String, String] = new KafkaProducer(properties)

  1 to 10 foreach {key =>
  val producerRecord = new ProducerRecord[String, String]("first_topic", key.toString, s"message with key $key")
  producer.send(producerRecord)
  }
  producer.flush()
  producer.close()

}
