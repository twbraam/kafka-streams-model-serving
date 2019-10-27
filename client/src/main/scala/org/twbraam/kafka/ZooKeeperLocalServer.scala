package org.twbraam.kafka

import java.io.IOException

import org.apache.curator.test.TestingServer

case class ZooKeeperLocalServer(port: Int, cleanOnStart: Boolean) {

  import KafkaLocalServer._
  import ZooKeeperLocalServer._

  private var zooKeeper: TestingServer = _

  def start(): Unit = {
    val zookeeperDataDir = dataDirectory(ZookeeperDataFolderName)
    zooKeeper = new TestingServer(port, zookeeperDataDir, false)
    Log.info(s"Zookeeper data directory is $zookeeperDataDir.")

    if (cleanOnStart) deleteDirectory(zookeeperDataDir)

    zooKeeper.start() // blocking operation
  }

  def stop(): Unit = {
    if (zooKeeper != null)
      try {
        zooKeeper.stop()
        zooKeeper = null
      }
      catch {
        case _: IOException => () // nothing to do if an exception is thrown while shutting down
      }
  }

  def getPort: Int = port
}

object ZooKeeperLocalServer {
  final val DefaultPort = 2181
  private final val ZookeeperDataFolderName = "zookeeper_data"
}
