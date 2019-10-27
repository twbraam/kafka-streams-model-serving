package org.twbraam.kafkaStreams.queriablestate

import java.net.InetAddress
import java.util

import org.twbraam.kafkaStreams.store.HostStoreInfo
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.{HostInfo, StreamsMetadata}
import org.twbraam.configuration.KafkaParameters

import scala.collection.JavaConverters._

/**
 * Looks up StreamsMetadata from KafkaStreams and converts the results
 * into Beans that can be JSON serialized via Jersey.
 * @see https://github.com/confluentinc/examples/blob/3.2.x/kafka-streams/src/main/java/io/confluent/examples/streams/interactivequeries/MetadataService.java
 */
class MetadataService(streams: KafkaStreams) {

  /**
    * Get the metadata for all instances of this Kafka Streams application that currently
    * has the provided store.
    *
    * @param store The store to locate
    * @return List of { @link HostStoreInfo}
    */
  def streamsMetadataForStore(store: String, port: Int): util.List[HostStoreInfo] = { // Get metadata for all of the instances of this Kafka Streams application hosting the store
    val metadata = streams.allMetadataForStore(store).asScala.toSeq match{
      case list if list.nonEmpty => list
      case _ => Seq(new StreamsMetadata(
        new HostInfo("localhost", port),
        new util.HashSet[String](util.Arrays.asList(KafkaParameters.STORE_NAME)), util.Collections.emptySet[TopicPartition]))
    }
    mapInstancesToHostStoreInfo(metadata)
  }


  private def mapInstancesToHostStoreInfo(metadatas: Seq[StreamsMetadata]): util.List[HostStoreInfo] = metadatas.map(convertMetadata).asJava

  private def convertMetadata(metadata: StreamsMetadata) : HostStoreInfo = {
    val currentHost = metadata.host match{
      case host if host equalsIgnoreCase "localhost" =>
        try{InetAddress.getLocalHost.getHostAddress}
        catch {case t: Throwable => ""}
      case host => host
    }
     HostStoreInfo(currentHost, metadata.port, metadata.stateStoreNames.asScala.toSeq)
  }
}
