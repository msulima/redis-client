package pl.msulima.redis.benchmark.repository

import java.util.function.Consumer

import pl.msulima.redis.benchmark.jedis.JedisClient

import scala.concurrent.{ExecutionContext, Future, Promise}

trait JedisJavaPipelinedRepositoryComponent {


  class JedisJavaPipelinedRepository(implicit ec: ExecutionContext) extends Repository {

    private val client = new JedisClient("localhost", 5000, 10)

    override def mget(keys: Seq[String]): Future[Seq[Payload]] = {
      Future.traverse(keys)(key => {
        val p = Promise[Payload]()
        client.get(key.getBytes, new Consumer[Array[Byte]] {
          override def accept(t: Array[Byte]) = {
            p.success(t)
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
