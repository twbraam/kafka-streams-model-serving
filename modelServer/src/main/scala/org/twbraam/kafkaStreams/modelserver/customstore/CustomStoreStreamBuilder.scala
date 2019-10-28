package org.twbraam.kafkaStreams.modelserver.customstore

import java.util
import java.util.Properties

import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.twbraam.configuration.KafkaParameters._
import org.twbraam.kafkaStreams.store.store.custom.ModelStateStoreBuilder
import org.twbraam.modelServer.model.{DataRecord, ModelToServe, ModelWithDescriptor, ServingResult}

/**
 * Use the Kafka Streams DSL to define the application streams.
 * Use a custom storage implementation for the running state.
 */
object CustomStoreStreamBuilder {

  def createStreamsFluent(streamsConfiguration: Properties): KafkaStreams = { // Create topology

    // Store definition
    val logConfig = new util.HashMap[String, String]
    val storeBuilder: ModelStateStoreBuilder = new ModelStateStoreBuilder(STORE_NAME).withLoggingEnabled(logConfig)

    // Create Stream builder
    val builder = new StreamsBuilder
    // Data input streams
    val data  = builder.stream[Array[Byte], Array[Byte]](DATA_TOPIC)
    val models  = builder.stream[Array[Byte], Array[Byte]](MODELS_TOPIC)

    // DataStore
    builder.addStateStore(storeBuilder)


    // Data Processor
    data
      .mapValues(value => DataRecord.fromByteArray(value))
      .filter((_, value) => value.isSuccess)
      .transform[Array[Byte], ServingResult](() => new DataProcessor, STORE_NAME)
      .mapValues(value => {
        if (value.processed) println(s"Calculated quality - ${value.result} calculated in ${value.duration} ms")
        else println("No model available - skipping")
        value
      })
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

    // Exercise:
    // Like all good production code, we're ignoring errors ;) in the `data` and `models` code. That is, we filter to keep
    // messages where `value.isSuccess` is true and ignore those that fail.
    // Use the `split` operator to split the stream into good and bad values.
    //   https://developer.lightbend.com/docs/api/kafka-streams-scala/0.2.1/com/lightbend/kafka/scala/streams/KStreamS.html#split(predicate:(K,V)=%3EBoolean):(com.lightbend.kafka.scala.streams.KStreamS[K,V],com.lightbend.kafka.scala.streams.KStreamS[K,V])
    // Write the bad values to stdout or to a special Kafka topic.
    // See the implementation of `DataRecord`, where we inject fake errors. Add the same logic to `ModelToServe` and
    // `ModelWithDescriptor`.

    // Exercise:
    // Print messages that scroll out of view is not very good for error handling. Write the errors to a special Kafka topic.

  }
}
