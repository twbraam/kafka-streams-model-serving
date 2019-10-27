package main.scala.org.twbraam.kafkaStreams.store.store.custom

import com.lightbend.scala.modelServer.model.ModelToServeStats

trait ReadableModelStateStore {
  def getCurrentServingInfo: ModelToServeStats
}
