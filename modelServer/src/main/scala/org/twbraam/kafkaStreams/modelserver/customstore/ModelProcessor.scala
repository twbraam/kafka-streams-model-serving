package org.twbraam.kafkaStreams.modelserver.customstore

import java.util.Objects

import org.apache.kafka.streams.processor.{AbstractProcessor, ProcessorContext}
import org.twbraam.configuration.KafkaParameters
import org.twbraam.modelServer.model.{ModelToServeStats, ModelWithDescriptor}
import org.twbraam.kafkaStreams.store.store.custom.ModelStateStore

import scala.util.Try

/**
 * Handle new model parameters; updates the current model used for scoring.
 */
class ModelProcessor extends AbstractProcessor[Array[Byte], Try[ModelWithDescriptor]] {

  private var modelStore: ModelStateStore = null

  import KafkaParameters._
  override def process (key: Array[Byte], modelWithDescriptor: Try[ModelWithDescriptor]): Unit = {

    modelStore.state.newModel = Some(modelWithDescriptor.get.model)
    modelStore.state.newState = Some(ModelToServeStats(modelWithDescriptor.get.descriptor))
  }

  override def init(context: ProcessorContext): Unit = {
    modelStore = context.getStateStore(STORE_NAME).asInstanceOf[ModelStateStore];
    Objects.requireNonNull(modelStore, "State store can't be null")
  }

}
