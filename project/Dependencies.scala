import sbt._

object Dependencies {
  val kafka = "org.apache.kafka" %% "kafka" % "2.3.1"
  val kafkaStreams = "org.apache.kafka" % "kafka-streams" % "2.3.1"
  val kafkaStreamsScala = "org.apache.kafka" %% "kafka-streams-scala" % "2.3.1"

  val curator = "org.apache.curator" % "curator-test" % "4.2.0"

  val tensorflow = "org.tensorflow" % "tensorflow" % "1.15.0"
  val jpmml = "org.jpmml" % "pmml-evaluator" % "1.4.1"
  val jpmmlextras = "org.jpmml" % "pmml-evaluator-extension" % "1.4.1"
  val slf4j = "org.slf4j" % "slf4j-simple" % "1.7.26"

  val modelsDependencies = Seq(jpmml, jpmmlextras, tensorflow, slf4j)
}

