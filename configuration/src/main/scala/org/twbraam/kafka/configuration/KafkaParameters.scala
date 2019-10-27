package org.twbraam.kafka.configuration

object KafkaParameters {
  val KAFKA_BROKER = "localhost:9092"

  val STORE_NAME = "ModelStore"
  val STORE_ID = 42

  val DATA_TOPIC = "mdata"
  val MODELS_TOPIC = "models"

  val DATA_GROUP = "wineRecordsGroup"
  val MODELS_GROUP = "modelRecordsGroup"
}
