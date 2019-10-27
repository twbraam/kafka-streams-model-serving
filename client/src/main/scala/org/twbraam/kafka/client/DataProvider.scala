package org.twbraam.kafka.client

import java.io.{ByteArrayOutputStream, File}
import java.nio.file.{Files, Paths}

import com.google.protobuf.ByteString
import org.twbraam.configuration.KafkaParameters._
import org.twbraam.kafka.{KafkaLocalServer, KafkaMessageSender}
import org.twbraam.model.modeldescriptor.ModelDescriptor
import org.twbraam.model.winerecord.WineRecord

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.Source

object DataProvider {
  val file = "data/winequality_red.csv"
  var dataTimeInterval: Int = 1000 * 1 // 1 sec
  val directory = "data/"
  val tensorFile = "data/optimized_WineQuality.pb"
  var modelTimeInterval: Int = 1000 * 60 * 1 // 5 mins

  def main(args: Array[String]) {

    println(s"Using kafka brokers at $KAFKA_BROKER")
    println(s"Data Message delay $dataTimeInterval")
    println(s"Model Message delay $modelTimeInterval")

    val kafka: KafkaLocalServer = KafkaLocalServer(true)
    kafka.start()
    kafka.createTopic(DATA_TOPIC)
    kafka.createTopic(MODELS_TOPIC)

    println(s"Cluster created")

    publishData()
    publishModels()

    while (true)
      pause(600000)
  }

  def publishData(): Future[Unit] = Future {

    val sender = KafkaMessageSender(KAFKA_BROKER)
    val bos = new ByteArrayOutputStream()
    val records: Iterator[WineRecord] = getListOfDataRecords(file)

    var nrec = 0
    while (true) {
      records.foreach(r => {
        bos.reset()
        r.writeTo(bos)
        sender.writeValue(DATA_TOPIC, bos.toByteArray)
        nrec = nrec + 1
        if (nrec % 10 == 0)
          println(s"printed $nrec records")
        pause(dataTimeInterval)
      })
    }
  }

  def publishModels(): Future[Unit] = Future {

    val sender = KafkaMessageSender(KAFKA_BROKER)
    val files = getListOfModelFiles(directory)
    val bos = new ByteArrayOutputStream()
    while (true) {
      files.foreach(f => {
        // PMML
        val pByteArray = Files.readAllBytes(Paths.get(directory + f))
        val pRecord = ModelDescriptor(
          name = f.dropRight(5),
          description = "generated from SparkML", modeltype = ModelDescriptor.ModelType.PMML,
          dataType = "wine").withData(ByteString.copyFrom(pByteArray))
        bos.reset()
        pRecord.writeTo(bos)
        sender.writeValue(MODELS_TOPIC, bos.toByteArray)
        println(s"Published Model ${pRecord.description}")
        pause(modelTimeInterval)
      })
      // TF
      val tByteArray = Files.readAllBytes(Paths.get(tensorFile))
      val tRecord = ModelDescriptor(name = tensorFile.dropRight(3),
        description = "generated from TensorFlow", modeltype = ModelDescriptor.ModelType.TENSORFLOW,
        dataType = "wine").withData(ByteString.copyFrom(tByteArray))
      bos.reset()
      tRecord.writeTo(bos)
      sender.writeValue(MODELS_TOPIC, bos.toByteArray)
      println(s"Published Model ${tRecord.description}")
      pause(modelTimeInterval)
    }
  }

  private def pause(timeInterval: Long): Unit =
    try {
      Thread.sleep(timeInterval)
    } catch {
      case _: Throwable => // Ignore
    }

  def getListOfDataRecords(file: String): Iterator[WineRecord] = {
    val bufferedSource = Source.fromFile(file)
    for {
      line <- bufferedSource.getLines
      cols = line.split(";").map(_.trim)
    } yield new WineRecord(
      fixedAcidity = cols(0).toDouble,
      volatileAcidity = cols(1).toDouble,
      citricAcid = cols(2).toDouble,
      residualSugar = cols(3).toDouble,
      chlorides = cols(4).toDouble,
      freeSulfurDioxide = cols(5).toDouble,
      totalSulfurDioxide = cols(6).toDouble,
      density = cols(7).toDouble,
      pH = cols(8).toDouble,
      sulphates = cols(9).toDouble,
      alcohol = cols(10).toDouble,
      dataType = "wine"
    )
  }

  private def getListOfModelFiles(dir: String): Seq[String] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory)
      d.listFiles.filter(f => f.isFile && f.getName.endsWith(".pmml")).map(_.getName)
    else
      Seq.empty[String]
  }
}
