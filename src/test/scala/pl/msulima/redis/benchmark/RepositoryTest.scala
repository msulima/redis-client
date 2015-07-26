package pl.msulima.redis.benchmark

import java.time.Instant
import java.time.temporal.ChronoField

import akka.actor.ActorSystem
import org.scalatest._
import pl.msulima.redis.benchmark.AvroItemSerDe
import pl.msulima.redis.benchmark.domain.Item

import scala.concurrent.Await
import scala.concurrent.duration._

class RepositoryTest extends FlatSpec with Matchers with RepositoryRegistry {

  override implicit val system = ActorSystem()
  private val serializer = new AvroItemSerDe

  "Repository" should "mset and mget items" in {
    // given
    val now = Instant.now().`with`(ChronoField.MICRO_OF_SECOND, 0)
    val item = Item(id = "id",
      name = "name", price = BigDecimal("11.99"), "seller", 12,
      Some("http://image"), now, now.plusSeconds(3))

    val repository = jedisMultiGetRepository

    // when
    Await.result(repository.mset(Seq(
      "1" -> serializer.serialize(item)
    )), 1.second)
    val items = Await.result(repository.mget(Seq("1", "nonexistent")), 1.second).map(serializer.deserialize)

    // then
    items should be(Seq(item))
  }
}
