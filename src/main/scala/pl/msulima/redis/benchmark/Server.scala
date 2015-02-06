package pl.msulima.redis.benchmark

import akka.actor.ActorSystem
import akka.http.Http
import akka.http.marshalling.Marshaller._
import akka.http.server.{Directives, Route}
import akka.stream.ActorFlowMaterializer

object Server extends App with Directives {

  private implicit val system = ActorSystem()
  private implicit val ec = system.dispatcher
  private implicit val materializer = ActorFlowMaterializer()

  val route: Route =
    path("order" / IntNumber) { id =>
      get {
        complete {
          "Received GET request for order " + id
        }
      } ~
        put {
          complete {
            "Received PUT request for order " + id
          }
        }
    }

  val serverBinding = Http(system).bind(interface = "localhost", port = 8080)
  serverBinding.startHandlingWith(route)
}
