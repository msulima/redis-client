package pl.msulima.redis.benchmark.repository

import java.util.function.Function

import pl.msulima.redis.benchmark.jedis.JedisClient

import scala.concurrent.{ExecutionContext, Future, Promise}

trait JedisJavaPipelinedRepositoryComponent {


  class JedisJavaPipelinedRepository(implicit ec: ExecutionContext) extends Repository {

    private val client = new JedisClient(5000)

    override def mget(keys: Seq[String]): Future[Seq[Payload]] = {
      Future.traverse(keys)(key => {
        val p = Promise[Payload]()
        client.get(key.getBytes, new Function[Array[Byte], Void] {
          override def apply(t: Array[Byte]): Void = {
            p.success(t)

            null
          }
        })
        p.future
      }).map(_.filter(_ != null))
    }

    override def mset(keys: Seq[(String, Payload)]): Future[Seq[(String, Payload)]] = {
      Future.traverse(keys)(key => {
        val p = Promise[(String, Payload)]()
        client.set(key._1.getBytes, key._2, new Runnable {
          override def run(): Unit = {
            p.success(key)
          }
        })
        p.future
      })
    }
  }

}
