package pl.msulima.redis.benchmark.repository

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.util.{ByteString, Timeout}
import brando._

import scala.concurrent.{ExecutionContext, Future}

trait BrandoMultiGetRepositoryComponent {

  class BrandoMultiGetRepository(system: ActorSystem)(implicit ec: ExecutionContext) extends Repository {

    private val redis = system.actorOf(Brando("localhost", 6379))
    private implicit val timeout = Timeout(1, TimeUnit.SECONDS)

    override def mget(keys: Seq[String]): Future[Seq[Array[Byte]]] = {
      if (keys.size == 1) {
        (redis ? Request("GET", keys.head)).mapTo[Option[ByteString]].map(response => {
          response.map(v => Seq(v.toArray)).getOrElse(Nil)
        })
      } else {
        for (Response.AsByteSeqs(values) <- redis ? Request("MGET", keys: _*)) yield values.map(_.toArray)
      }
    }

    override def mset(keys: Seq[(String, Array[Byte])]) = {
      (redis ? Request(ByteString("MSET"), keys.flatMap(k => Seq(ByteString(k._1), ByteString(k._2))): _*)).map(_ => keys)
    }
  }

}
