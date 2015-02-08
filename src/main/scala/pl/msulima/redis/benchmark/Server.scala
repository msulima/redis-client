package pl.msulima.redis.benchmark

import akka.actor.ActorSystem
import akka.http.Http
import akka.http.marshalling.Marshaller._
import akka.http.server.{Directives, Route}
import akka.stream.ActorFlowMaterializer

import scala.concurrent.Future
import scala.concurrent.forkjoin.ThreadLocalRandom

object Server extends App with Directives
with JedisMultiGetRepositoryComponent
with BrandoMultiGetRepositoryComponent
with JedisAkkaPipelinedRepositoryComponent {

  private val GroupSize = 5

  private implicit val system = ActorSystem()
  private implicit val ec = system.dispatcher
  private implicit val materializer = ActorFlowMaterializer()

  private val jedisMultiGetRepository = new JedisMultiGetRepository
  private val jedisPipelinedRepository = new JedisAkkaPipelinedRepository(system)
  private val brandoMultiGetRepository = new BrandoMultiGetRepository(system)

  private def testRoute(name: String, sut: Repository) =
    pathPrefix(name) {
      path("concrete" / Rest) { (id: String) =>
        get {
          complete {
            sut.get(Seq(id)).map(_.toString())
          }
        }
      } ~ path("random" / IntNumber) { (id: Int) =>
        get {
          complete {
            val keys = KeysGenerator.get(id)

            val sequence = Future.sequence(keys.grouped(GroupSize).map(sut.get(_))).map(_.flatten.toSeq)

            sequence.map(keys.zip(_).toString())
          }
        } ~ put {
          complete {
            sut.set(KeysGenerator.set(id)).map(_.toString())
          }
        }
      }
    }

  private val route: Route =
    pathPrefix("jedis") {
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

  def get(n: Int) = (1 to n).map(_ => ThreadLocalRandom.current().nextInt(MaxId).toString)

  def set(n: Int) = get(n).zip(get(n))
}
