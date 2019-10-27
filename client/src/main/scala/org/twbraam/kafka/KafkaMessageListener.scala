package org.twbraam.kafka

import java.time.Duration.ofMillis

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.twbraam.kafka.client.RecordProcessorTrait

import scala.collection.JavaConverters._

object KafkaMessageListener {
  private val AUTOCOMMIT_INTERVAL = "1000" // Frequency off offset commits
  private val SESSION_TIMEOUT = "30000" // The timeout used to detect failures - should be greater then processing time
  private val MAX_POLL_RECORDS = "10" // Max number of records consumed in a single poll

  def consumerProperties(brokers: String, group: String, keyDeserealizer: String, valueDeserializer: String): Map[String, Object] = {
    Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
      ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG -> AUTOCOMMIT_INTERVAL,
      ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG -> SESSION_TIMEOUT,
      ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> MAX_POLL_RECORDS,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> keyDeserealizer,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> valueDeserializer
    )
  }

  def apply[K, V](brokers: String, topic: String, group: String,
                  processor: RecordProcessorTrait[K, V]): KafkaMessageListener[K, V] =
    new KafkaMessageListener[K, V](brokers, topic, group, classOf[ByteArrayDeserializer].getName, classOf[ByteArrayDeserializer].getName, processor)
}

class KafkaMessageListener[K, V](brokers: String, topic: String, group: String, keyDeserealizer: String, valueDeserealizer: String,
                                 processor: RecordProcessorTrait[K, V]) extends Runnable {

  import KafkaMessageListener._

  val consumer: KafkaConsumer[K, V] = new KafkaConsumer[K, V](consumerProperties(brokers, group, keyDeserealizer, valueDeserealizer).asJava)
  consumer.subscribe(Seq(topic).asJava)
  var completed = false

  def complete(): Unit = {
    completed = true
  }

  override def run(): Unit = {
    while (!completed) {
      val records = consumer.poll(ofMillis(100))
      records.asScala.foreach(processor.processRecord)
    }
    consumer.close()
    System.out.println("Listener completes")
  }

  def start(): Unit = {
    val t = new Thread(this)
    t.start()
  }
}
