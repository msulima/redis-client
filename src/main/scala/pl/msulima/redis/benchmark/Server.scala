package pl.msulima.redis.benchmark

import java.time.Instant

import akka.actor.ActorSystem
import pl.msulima.redis.benchmark.domain.Item
import pl.msulima.redis.benchmark.repository._
import pl.msulima.redis.benchmark.serialization.AvroItemSerDe
import spray.routing.{Route, SimpleRoutingApp}

import scala.concurrent.Future
import scala.concurrent.forkjoin.ThreadLocalRandom

object Server extends App with RepositoryRegistry with SimpleRoutingApp {

  override implicit val system = ActorSystem()
  private val serializer = new AvroItemSerDe

  private def testRoute(name: String, sut: Repository) =
    pathPrefix(name) {
      path("concrete" / Rest) { (id: String) =>
        get {
          complete {
            sut.mget(Seq(id)).map(item => item.headOption.map(i => serializer.toJSON(i)))
          }
        }
      } ~ path("random" / IntNumber) { (id: Int) =>
        get {
          complete {
            val keys = KeysGenerator.get(id)

            val sequence = Future.sequence(keys.grouped(KeysGenerator.GroupSize).map(sut.mget(_))).map(_.flatten.toSeq)

            sequence.map(i => keys.zip(i.map(serializer.deserialize)).toString())
          }
        } ~ put {
          complete {
            sut.mset(KeysGenerator.set(id).map(k => {
              k._1 -> serializer.serialize(k._2)
            })).map(_.map(k => k._1 -> serializer.deserialize(k._2)).toString())
          }
        }
      }
    }

  private val route: Route =
    pathPrefix("jedis") {
      testRoute("akka-pipelined", jedisAkkaPipelinedRepository) ~
        testRoute("akka-batch", jedisAkkaBatchRepository) ~
        testRoute("pipelined", jedisPipelinedRepository) ~
        testRoute("multi", jedisMultiGetRepository)
    } ~ pathPrefix("brando") {
      testRoute("multi", brandoMultiGetRepository)
    } ~ pathPrefix("netty") {
      testRoute("simple", nettyRepository)
    }

  startServer("localhost", port = 8080)(route)
}

object KeysGenerator {

  val GroupSize = 1

  private val MaxId = 1000000

  def get(n: Int) = (1 to n).map(_ => ThreadLocalRandom.current().nextInt(MaxId).toString)

  def set(n: Int) = get(n).map(id => {
    val item = Item(id = id,
      name = "name", price = BigDecimal("11.99"), "seller", ThreadLocalRandom.current().nextInt(100),
      Some("http://image"), Instant.now(), Instant.now().plusSeconds(60))

    id -> item
  })
}
