package org.twbraam.kafkaStreams.queriablestate.inmemory

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import akka.util.Timeout
import de.heikoseeberger.akkahttpjackson.JacksonSupport
import org.apache.kafka.streams.KafkaStreams
import org.twbraam.kafkaStreams.store.StoreState
import org.twbraam.modelServer.model.ModelToServeStats

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}

/**
 *  A simple REST proxy that runs embedded in the Model server. This is used to
 *  demonstrate how a developer can use the Interactive Queries APIs exposed by Kafka Streams to
 *  locate and query the State Stores within a Kafka Streams Application.
 *  @see https://github.com/confluentinc/examples/blob/3.2.x/kafka-streams/src/main/java/io/confluent/examples/streams/interactivequeries/WordCountInteractiveQueriesRestService.java
 */
object RestServiceInMemory{

  implicit val system: ActorSystem = ActorSystem("ModelServing")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher
  implicit val timeout: Timeout = Timeout(10.seconds)
  val host = "127.0.0.1"
  val port = 8888
  val routes: Route = QueriesResource.storeRoutes()

  // Surf to http://localhost:8888/state/instances for the list of currently deployed instances.
  // Then surf to http://localhost:8888/state/value for the current state of execution for a given model.
  def startRestProxy(streams: KafkaStreams, port: Int): Future[Unit] = {

    Http().bindAndHandle(routes, host, port) map
      { binding => println(s"Starting models observer on port ${binding.localAddress}") } recover {
      case ex =>
        println(s"Models observer could not bind to $host:$port - ${ex.getMessage}")
    }
  }
}

// Surf to http://localhost:8888/state/value

object QueriesResource extends JacksonSupport {

  def storeRoutes(): Route =
    get {
      pathPrefix("state") {
        path("value") {
          val info: ModelToServeStats = StoreState().currentState.getOrElse(ModelToServeStats.empty)
          complete(info)
        }
      }
    }
}
