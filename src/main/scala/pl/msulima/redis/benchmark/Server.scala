package pl.msulima.redis.benchmark

import java.time.Instant

import akka.actor.ActorSystem
import akka.http.Http
import akka.http.marshalling.Marshaller._
import akka.http.server.{Directives, Route}
import akka.stream.ActorFlowMaterializer
import pl.msulima.redis.benchmark.domain.Item
import pl.msulima.redis.benchmark.repository._
import pl.msulima.redis.benchmark.serialization.AvroItemSerDe

import scala.concurrent.Future
import scala.concurrent.forkjoin.ThreadLocalRandom

object Server extends App with Directives with RepositoryRegistry {

  private val GroupSize = 5

  override implicit val system = ActorSystem()
  private implicit val ec = system.dispatcher
  private implicit val materializer = ActorFlowMaterializer()

  private def testRoute(name: String, sut: Repository) =
    pathPrefix(name) {
      path("concrete" / Rest) { (id: String) =>
        get {
          complete {
            sut.mget(Seq(id)).map(_.toString())
          }
        }
      } ~ path("random" / IntNumber) { (id: Int) =>
        get {
          complete {
            val keys = KeysGenerator.get(id)

            val sequence = Future.sequence(keys.grouped(GroupSize).map(sut.mget(_))).map(_.flatten.toSeq)

            sequence.map(keys.zip(_).toString())
          }
        } ~ put {
          complete {
            sut.mset(KeysGenerator.set(id)).map(_.toString())
          }
        }
      }
    }

  private val route: Route =
    pathPrefix("jedis") {
      testRoute("akka-pipelined", jedisAkkaPipelinedRepository) ~
        testRoute("pipelined", jedisPipelinedRepository) ~
        testRoute("multi", jedisMultiGetRepository)
    } ~ pathPrefix("brando") {
      testRoute("multi", brandoMultiGetRepository)
    }

  val serverBinding = Http(system).bind(interface = "localhost", port = 8080)
  serverBinding.startHandlingWith(route)
}

object KeysGenerator {

  private val MaxId = 1000000
  private val serializer = new AvroItemSerDe

  def get(n: Int) = (1 to n).map(_ => ThreadLocalRandom.current().nextInt(MaxId).toString)

  def set(n: Int) = get(n).map(id => {
    val item = Item(id = "id",
      name = "name", price = BigDecimal("11.99"), "seller", 12,
      Some("http://image"), Instant.now(), Instant.now().plusSeconds(60))

    id -> serializer.serialize(item)
  })
}
