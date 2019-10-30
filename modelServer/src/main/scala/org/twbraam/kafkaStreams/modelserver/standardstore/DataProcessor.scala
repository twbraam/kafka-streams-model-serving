package org.twbraam.kafkaStreams.modelserver.standardstore

import java.util.Objects

import org.twbraam.configuration.KafkaParameters
import org.twbraam.model.winerecord.WineRecord
import org.twbraam.modelServer.model.ServingResult
import org.twbraam.kafkaStreams.store.StoreState
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore

import scala.util.Try

/**
 * The DataProcessor for the "standard" state store, the one provided by Kafka Streams.
 * See also this example:
 * https://github.com/bbejeck/kafka-streams/blob/master/src/main/java/bbejeck/processor/stocks/StockSummaryProcessor.java
 */
class DataProcessor extends Transformer[Array[Byte], Try[WineRecord], KeyValue[Array[Byte], ServingResult]] {

  private var modelStore: KeyValueStore[Integer, StoreState] = _

  import KafkaParameters._

  // Exercise:
  // See the exercises described in com.lightbend.scala.kafkastreams.modelserver.customstore.DataProcessor.
  override def transform(key: Array[Byte], dataRecord: Try[WineRecord]) : KeyValue[Array[Byte], ServingResult] = {

    var state = modelStore.get(STORE_ID)
    if (state == null) state = new StoreState

    state.newModel match {
      case Some(model) =>
        // close current model first
        state.currentModel match {
          case Some(m) => m.cleanup()
          case _ =>
        }
        // Update model
        state.currentModel = Some(model)
        state.currentState = state.newState
        state.newModel = None
      case _ =>
    }
    val result = state.currentModel match {
      case Some(model) =>
        val start: Long = System.currentTimeMillis()
        val quality: Double = model.score(dataRecord.get).getOrElse(0.0)
        val duration: Long = System.currentTimeMillis() - start

        //        println(s"Calculated quality - $quality calculated in $duration ms")
        state.currentState = state.currentState.map(_.incrementUsage(duration))
        ServingResult(processed = true, quality, duration)
      case _ =>
        //        println("No model available - skipping")
        ServingResult(processed = false)
    }
    modelStore.put(STORE_ID, state)
    new KeyValue(key, result)
  }

  override def init(context: ProcessorContext): Unit = {
    modelStore = context.getStateStore(STORE_NAME).asInstanceOf[KeyValueStore[Integer, StoreState]]
    Objects.requireNonNull(modelStore, "State store can't be null")
  }

  override def close(): Unit = {}

  def punctuate(timestamp: Long): (Array[Byte], ServingResult) = null
}