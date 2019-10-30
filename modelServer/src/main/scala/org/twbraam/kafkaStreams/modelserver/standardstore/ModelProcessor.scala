package org.twbraam.kafkaStreams.modelserver.standardstore

import java.util.Objects

import org.apache.kafka.streams.processor.{AbstractProcessor, ProcessorContext}
import org.apache.kafka.streams.state.KeyValueStore
import org.twbraam.configuration.KafkaParameters
import org.twbraam.kafkaStreams.store.StoreState
import org.twbraam.modelServer.model.{ModelToServeStats, ModelWithDescriptor}

import scala.util.Try

/**
 * Handle new model parameters; updates the current model used for scoring.
 */
class ModelProcessor extends AbstractProcessor[Array[Byte], Try[ModelWithDescriptor]] {

  private var modelStore: KeyValueStore[Integer, StoreState] = null

  import KafkaParameters._
  override def process (key: Array[Byte], modelWithDescriptor: Try[ModelWithDescriptor]): Unit = {

    var state = modelStore.get(STORE_ID)
    if (state == null) state = new StoreState

    state.newModel = Some(modelWithDescriptor.get.model)
    state.newState = Some(ModelToServeStats(modelWithDescriptor.get.descriptor))
    modelStore.put(KafkaParameters.STORE_ID, state)
  }

  override def init(context: ProcessorContext): Unit = {
    modelStore = context.getStateStore(STORE_NAME).asInstanceOf[KeyValueStore[Integer, StoreState]]
    Objects.requireNonNull(modelStore, "State store can't be null")
  }
}
