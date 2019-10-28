package org.twbraam.kafkaStreams.modelserver.standardstore

import java.util
import java.util.{HashMap, Properties}

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.internals.TransformerSupplierAdapter
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.kstream.{Predicate, TransformerSupplier, ValueMapper}
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.state.Stores
import org.twbraam.configuration.KafkaParameters._
import org.twbraam.kafkaStreams.modelserver._
import org.twbraam.kafkaStreams.store.store.ModelStateSerde
import org.twbraam.model.winerecord.WineRecord
import org.twbraam.modelServer.model.{DataRecord, ModelToServe, ModelWithDescriptor, ServingResult}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes._

import scala.util.Try

/**
 * Use the Kafka Streams DSL to define the application streams.
 * Use the built-in storage implementation for the running state.
 */
object StandardStoreStreamBuilder {


  def createStreamsFluent(streamsConfiguration: Properties) : KafkaStreams = { // Create topology

    // Store definition
    val logConfig = new util.HashMap[String, String]
    val storeSupplier = Stores.inMemoryKeyValueStore(STORE_NAME)
    val storeBuilder = Stores.keyValueStoreBuilder(storeSupplier, Serdes.Integer, new ModelStateSerde).withLoggingEnabled(logConfig)

    // Create Stream builder
    val builder = new StreamsBuilder
    // Data input streams
    val data  = builder.stream[Array[Byte], Array[Byte]](DATA_TOPIC)
    val models  = builder.stream[Array[Byte], Array[Byte]](MODELS_TOPIC)

    // DataStore
    builder.addStateStore(storeBuilder)


    // Data Processor

/*    val x = data
      .mapValues(value => DataRecord.fromByteArray(value))
    x
      .filter((key, value) => (value.isSuccess))
      .transform(() => new DataProcessor, STORE_NAME)
      .mapValues(value => {
        if(value.processed) println(s"Calculated quality - ${value.result} calculated in ${value.duration} ms")
        else println("No model available - skipping")
        value
      })*/

    TransformerSupplier
    TransformerSupplierFromFunction

    data
      .mapValues(value => DataRecord.fromByteArray(value))
      .filter((_, value) => value.isSuccess)
      .transform[Array[Byte], ServingResult](new DataProcessor, STORE_NAME)
      .mapValues { value: ServingResult => {
        if (value.processed) println(s"Calculated quality - ${value.result} calculated in ${value.duration} ms")
        else println("No model available - skipping")
        value
      }}
    // Exercise:
    // We just printed the result, but we didn't do anything else with it.
    // In particular, we might want to write the results to a new Kafka topic.
    // 1. Modify the "client" to create a new output topic.
    // 2. Modify KafkaModelServer to add the configuration for the new topic.
    // 3. Add a final step that writes the results to the new topic.
    //    Consult the Kafka Streams documentation for details.

    //Models Processor
    models
      .mapValues(value => ModelToServe.fromByteArray(value))
      .filter((_, value) => value.isSuccess)
      .mapValues(value => ModelWithDescriptor.fromModelToServe(value.get))
      .filter((_, value) => value.isSuccess)
      .process(() => new ModelProcessor, STORE_NAME)

    // Create and build topology
    val topology = builder.build
    println(topology.describe)

    new KafkaStreams(topology, streamsConfiguration)

  }
}
