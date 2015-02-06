package pl.msulima.redis.benchmark

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

trait JedisMultiGetRepositoryComponent {

  val jedisMultiGetRepository: Repository

  class JedisMultiGetRepository(implicit ec: ExecutionContext) extends Repository {
    private val pool = new JedisPool(new JedisPoolConfig(), "localhost")

    override def get(keys: Seq[String]) = Future {
      val jedis = pool.getResource
      try {
        jedis.mget(keys: _*).toSeq
      } catch {
        case NonFatal(ex) =>
          jedis.close()
          throw ex
      }
    }

    override def set(keys: Seq[(String, String)]) = Future {
      val jedis = pool.getResource
      try {
        jedis.mset(keys.flatMap(k => Seq(k._1, k._2)): _*).toSeq
        keys
      } catch {
        case NonFatal(ex) =>
          jedis.close()
          throw ex
      }
    }
  }

}
