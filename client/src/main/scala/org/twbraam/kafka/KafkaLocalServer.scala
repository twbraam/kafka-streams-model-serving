package org.twbraam.kafka

import java.io.File
import java.util.Properties

import kafka.server.{KafkaConfig, KafkaServerStartable}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, CreateTopicsOptions, NewTopic}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.reflect.io.Directory

/**
 * Helper class for working with a local, embedded instance of Kafka.
 */
case class KafkaLocalServer private(kafkaProperties: Properties, zooKeeperServer: ZooKeeperLocalServer) {

  // Exercise:
  // Create a similar set of classes for a real Kafka cluster. See the Kafka documentation for
  // configuring and running Kafka clusters and the Kafka Publisher documentation for
  // instructions on how to connect to the cluster. Can you create an abstraction that makes it
  // easy to switch between the local and "real" clusters?

  import KafkaLocalServer._

  val props = new Properties
  props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, s"localhost:${zooKeeperServer.getPort}")
  props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, DEFAULT_ZK_SESSION_TIMEOUT_MS)
  props.put(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, DEFAULT_ZK_CONNECTION_TIMEOUT_MS)
  // No replacement option for: isZkSecurityEnabled?

  private var broker: KafkaServerStartable = _
  private val client: AdminClient = AdminClient.create(props)


  def start(): Unit = {
    println("Starting")
    broker = KafkaServerStartable.fromProps(kafkaProperties)
    broker.startup()
  }

  def stop(): Unit = {
    println("Stopping")
    if (broker != null) {
      broker.shutdown()
      zooKeeperServer.stop()
      broker = null
    }
  }

  /**
   * Create a Kafka topic with 1 partition and a replication factor of 1.
   *
   * @param topic The name of the topic.
   */
  def createTopic(topic: String): Unit = {
    println(s"Creating topic: $topic")
    createTopic(topic, 1, 1, new CreateTopicsOptions)
  }

  /**
   * Create a Kafka topic with the given parameters.
   *
   * @param topic       The name of the topic.
   * @param partitions  The number of partitions for this topic.
   * @param replication The replication factor for (the partitions of) this topic.
   */
  def createTopic(topic: String, partitions: Int, replication: Int): Unit =
    createTopic(topic, partitions, replication, new CreateTopicsOptions)

  /**
   * Create a Kafka topic with the given parameters.
   *
   * @param topic       The name of the topic.
   * @param partitions  The number of partitions for this topic.
   * @param replication The replication factor for (partitions of) this topic.
   * @param topicConfig Additional topic-level configuration settings.
   */
  def createTopic(topic: String, partitions: Int, replication: Int, topicConfig: CreateTopicsOptions): Unit = {
    val newTopic = new NewTopic(topic, partitions, replication.toShort)
    client.createTopics(List(newTopic).asJavaCollection, topicConfig)
  }
}

object KafkaLocalServer {
  final val DefaultPort = 9092
  final val DefaultResetOnStart = true
  val DEFAULT_ZK_CONNECT = "localhost:2181"
  val DEFAULT_ZK_SESSION_TIMEOUT_MS: Integer = 10 * 1000
  val DEFAULT_ZK_CONNECTION_TIMEOUT_MS: Integer = 8 * 1000

  final val baseDir = "tmp/"

  final val KafkaDataFolderName = "kafka_data"

  val Log: Logger = LoggerFactory.getLogger(classOf[KafkaLocalServer])

  def apply(cleanOnStart: Boolean): KafkaLocalServer = apply(DefaultPort, ZooKeeperLocalServer.DefaultPort, cleanOnStart)

  def apply(kafkaPort: Int, zookeeperServerPort: Int, cleanOnStart: Boolean): KafkaLocalServer = {
    val kafkaDataDir = dataDirectory(KafkaDataFolderName)
    Log.info(s"Kafka data directory is $kafkaDataDir.")

    val kafkaProperties = createKafkaProperties(kafkaPort, zookeeperServerPort, kafkaDataDir)

    if (cleanOnStart) deleteDirectory(kafkaDataDir)
    val zk = new ZooKeeperLocalServer(zookeeperServerPort, cleanOnStart)
    zk.start()
    new KafkaLocalServer(kafkaProperties, zk)
  }

  /**
   * Creates a Properties instance for Kafka customized with values passed in argument.
   */
  private def createKafkaProperties(kafkaPort: Int, zookeeperServerPort: Int, dataDir: File): Properties = {
    val kafkaProperties = new Properties
    kafkaProperties.put(KafkaConfig.ListenersProp, s"PLAINTEXT://localhost:$kafkaPort")
    kafkaProperties.put(KafkaConfig.ZkConnectProp, s"localhost:$zookeeperServerPort")
    kafkaProperties.put(KafkaConfig.ZkConnectionTimeoutMsProp, "6000")
    kafkaProperties.put(KafkaConfig.BrokerIdProp, "0")
    kafkaProperties.put(KafkaConfig.NumNetworkThreadsProp, "3")
    kafkaProperties.put(KafkaConfig.NumIoThreadsProp, "8")
    kafkaProperties.put(KafkaConfig.SocketSendBufferBytesProp, "102400")
    kafkaProperties.put(KafkaConfig.SocketReceiveBufferBytesProp, "102400")
    kafkaProperties.put(KafkaConfig.SocketRequestMaxBytesProp, "104857600")
    kafkaProperties.put(KafkaConfig.NumPartitionsProp, "1")
    kafkaProperties.put(KafkaConfig.NumRecoveryThreadsPerDataDirProp, "1")
    kafkaProperties.put(KafkaConfig.OffsetsTopicReplicationFactorProp, "1")
    kafkaProperties.put(KafkaConfig.TransactionsTopicReplicationFactorProp, "1")
    kafkaProperties.put(KafkaConfig.LogRetentionTimeHoursProp, "2")
    kafkaProperties.put(KafkaConfig.LogSegmentBytesProp, "1073741824")
    kafkaProperties.put(KafkaConfig.LogCleanupIntervalMsProp, "300000")
    kafkaProperties.put(KafkaConfig.AutoCreateTopicsEnableProp, "true")
    kafkaProperties.put(KafkaConfig.ControlledShutdownEnableProp, "true")
    kafkaProperties.put(KafkaConfig.LogDirProp, dataDir.getAbsolutePath)

    kafkaProperties
  }

  def deleteDirectory(directory: File): Unit =
    if (!Directory(directory).deleteRecursively())
      Log.warn(s"Failed to delete directory ${directory.getAbsolutePath}")


  def dataDirectory(directoryName: String): File = {

    val dataDirectory = new File(baseDir + directoryName)
    if (dataDirectory.exists() && !dataDirectory.isDirectory)
      throw new IllegalArgumentException(s"Cannot use $directoryName as a directory name because a file with that name already exists in $dataDirectory.")

    dataDirectory
  }
}

