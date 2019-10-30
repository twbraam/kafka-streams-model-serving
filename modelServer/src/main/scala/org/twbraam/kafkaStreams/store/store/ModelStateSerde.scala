package org.twbraam.kafkaStreams.store.store

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.util

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.twbraam.kafkaStreams.store.StoreState
import org.twbraam.model.modeldescriptor.ModelDescriptor
import org.twbraam.modelServer.model.PMML.PMMLModel
import org.twbraam.modelServer.model.tensorflow.TensorFlowModel
import org.twbraam.modelServer.model.{ModelFactory, ModelToServeStats, ModelWithDescriptor}

/**
 * Serialization and deserialization of model stat information. Needed for durable storage. Based on
 * https://github.com/confluentinc/examples/blob/3.2.x/kafka-streams/src/main/scala/io/confluent/examples/streams/algebird/TopCMSSerde.scala
 */
class ModelStateSerde extends Serde[StoreState] {

  private val mserializer = new ModelStateSerializer()
  private val mdeserializer = new ModelStateDeserializer()

  override def deserializer(): ModelStateDeserializer = mdeserializer

  override def serializer(): ModelStateSerializer = mserializer

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}
}

object ModelStateDeserializer {
  val factories: Map[Int, ModelFactory] = Map(
    ModelDescriptor.ModelType.PMML.index -> PMMLModel,
    ModelDescriptor.ModelType.TENSORFLOW.index -> TensorFlowModel
  )
}

class ModelStateDeserializer extends Deserializer[StoreState] {

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): StoreState = {
    if(data != null) {
      val input = new DataInputStream(new ByteArrayInputStream(data))
      new StoreState(ModelWithDescriptor.readModel(input), ModelWithDescriptor.readModel(input),
        ModelToServeStats.readServingInfo(input), ModelToServeStats.readServingInfo(input))
    }
    else new StoreState()
  }

  override def close(): Unit = {}

}

class ModelStateSerializer extends Serializer[StoreState] {

  private val bos = new ByteArrayOutputStream()

  override def serialize(topic: String, state: StoreState): Array[Byte] = {
    bos.reset()
    val output = new DataOutputStream(bos)
    ModelWithDescriptor.writeModel(output, state.currentModel.orNull)
    ModelWithDescriptor.writeModel(output, state.newModel.orNull)
    ModelToServeStats.writeServingInfo(output, state.currentState.orNull)
    ModelToServeStats.writeServingInfo(output, state.newState.orNull)
    try {
      output.flush()
      output.close()
    } catch {
      case t: Throwable =>
    }
    bos.toByteArray
  }

  override def close(): Unit = {}

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}
}
