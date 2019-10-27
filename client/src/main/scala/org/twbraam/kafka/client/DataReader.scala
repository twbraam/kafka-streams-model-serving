package org.twbraam.kafka.client

import org.twbraam.kafka.KafkaMessageListener
import org.twbraam.kafka.configuration.KafkaParameters._

object DataReader {

  def main(args: Array[String]) {

    println(s"Using kafka brokers at $KAFKA_BROKER")

    val listener = KafkaMessageListener(KAFKA_BROKER, DATA_TOPIC, DATA_GROUP, new RecordProcessor())
    listener.start()
  }
}