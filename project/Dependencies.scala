import sbt._

object Dependencies {
  val kafka             = "org.apache.kafka"  %% "kafka"               % "2.3.1"
  val kafkaStreams      = "org.apache.kafka"   % "kafka-streams"       % "2.3.1"
  val kafkaStreamsScala = "org.apache.kafka"  %% "kafka-streams-scala" % "2.3.1"

  val akkaHttp            = "com.typesafe.akka"         %% "akka-http"                    % "10.1.10"
  val akkaHttpJsonJackson = "de.heikoseeberger"         %% "akka-http-jackson"            % "1.27.0"


  val curator           = "org.apache.curator" % "curator-test"        % "4.2.0"

  val tensorflow        = "org.tensorflow"     % "tensorflow"               % "1.15.0"
  val jpmml             = "org.jpmml"          % "pmml-evaluator"           % "1.4.1"
  val jpmmlextras       = "org.jpmml"          % "pmml-evaluator-extension" % "1.4.1"
  val slf4j             = "org.slf4j"          % "slf4j-simple"             % "1.7.26"

  val gson          = "com.google.code.gson"            % "gson"                          % "2.8.2"
  val jersey        = "org.glassfish.jersey.containers" % "jersey-container-servlet-core" % "2.25"
  val jerseymedia   = "org.glassfish.jersey.media"      % "jersey-media-json-jackson"     % "2.25"
  val jettyserver   = "org.eclipse.jetty"               % "jetty-server"                  % "9.4.7.v20170914"
  val jettyservlet  = "org.eclipse.jetty"               % "jetty-servlet"                 % "9.4.7.v20170914"
  val wsrs          = "javax.ws.rs"                     % "javax.ws.rs-api"               % "2.0.1"

  val modelsDependencies     = Seq(jpmml, jpmmlextras, tensorflow, slf4j)
  val webDependencies        = Seq(gson, jersey, jerseymedia, jettyserver, jettyservlet, wsrs, slf4j)
  val akkHTTPPSupport        = Seq(akkaHttp, akkaHttpJsonJackson, slf4j)

}