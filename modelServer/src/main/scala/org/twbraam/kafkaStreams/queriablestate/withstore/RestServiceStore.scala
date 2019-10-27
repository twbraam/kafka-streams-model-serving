package org.twbraam.kafkaStreams.queriablestate.withstore

import javax.ws.rs.NotFoundException
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import akka.util.Timeout
import org.twbraam.kafkaStreams.queriablestate.MetadataService
import org.twbraam.configuration.KafkaParameters._
import org.twbraam.kafkaStreams.store.StoreState
import org.twbraam.kafkaStreams.store.store.custom.ModelStateStoreType
import de.heikoseeberger.akkahttpjackson.JacksonSupport
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.QueryableStoreTypes

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.concurrent.duration._

/**
 *  A simple REST proxy that runs embedded in the Model server. This is used to
 *  demonstrate how a developer can use the Interactive Queries APIs exposed by Kafka Streams to
 *  locate and query the State Stores within a Kafka Streams Application.
 *  @see https://github.com/confluentinc/examples/blob/3.2.x/kafka-streams/src/main/java/io/confluent/examples/streams/interactivequeries/WordCountInteractiveQueriesRestService.java
 */
object RestServiceStore {

  implicit val system: ActorSystem = ActorSystem("ModelServing")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher
  implicit val timeout: Timeout = Timeout(10.seconds)
  val host = "127.0.0.1"
  val port = 8888

  def startRestProxy(streams: KafkaStreams, port: Int, storeType : String): Future[Unit] = {

    val routes: Route = QueriesResource.storeRoutes(streams, port, storeType)
    Http().bindAndHandle(routes, host, port) map
      { binding => println(s"Starting models observer on port ${binding.localAddress}") } recover {
      case ex =>
        println(s"Models observer could not bind to $host:$port - ${ex.getMessage}")
    }
  }
}

object QueriesResource extends JacksonSupport {

  private val customStoreType = new ModelStateStoreType()
  private val standardStoreType = QueryableStoreTypes.keyValueStore[Integer,StoreState]

  def storeRoutes(streams: KafkaStreams, port : Int, storeType : String): Route = {
    val metadataService = new MetadataService(streams)
    get {
      pathPrefix("state") {
        path("instances") {
          complete(
            metadataService.streamsMetadataForStore(STORE_NAME, port)
          )
        } ~
          path("value") {
            storeType match {
              case "custom" =>
                val store = streams.store(STORE_NAME, customStoreType)
                if (store == null) throw new NotFoundException
                complete(store.getCurrentServingInfo)
              case _ =>
                val store = streams.store(STORE_NAME, standardStoreType)
                if (store == null) throw new NotFoundException
                complete(store.get(STORE_ID).currentState)
            }
          }
        }
    }
  }
}
