package pl.msulima.redis.benchmark

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.util.Timeout
import brando._

import scala.concurrent.ExecutionContext

trait BrandoMultiGetRepositoryComponent {

  class BrandoMultiGetRepository(system: ActorSystem)(implicit ec: ExecutionContext) extends Repository {

    private val redis = system.actorOf(Brando("localhost", 6379))
    private implicit val timeout = Timeout(1, TimeUnit.SECONDS)

    override def get(keys: Seq[String]) = {
      for (Response.AsStringOptions(values) <- redis ? Request("MGET", keys: _*)) yield values.map(_.getOrElse("null"))
    }

    override def set(keys: Seq[(String, String)]) = {
      (redis ? Request("MSET", keys.flatMap(k => Seq(k._1, k._2)): _*)).map(_ => keys)
    }
  }

}
