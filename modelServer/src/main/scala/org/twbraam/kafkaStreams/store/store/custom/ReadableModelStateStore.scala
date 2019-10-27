package org.twbraam.kafkaStreams.store.store.custom

import org.twbraam.modelServer.model.ModelToServeStats

trait ReadableModelStateStore {
  def getCurrentServingInfo: ModelToServeStats
}
