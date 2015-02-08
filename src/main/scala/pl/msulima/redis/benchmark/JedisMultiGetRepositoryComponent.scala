package pl.msulima.redis.benchmark

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

trait JedisMultiGetRepositoryComponent {

  class JedisMultiGetRepository(implicit ec: ExecutionContext) extends Repository {
    private val pool = new JedisPool(new JedisPoolConfig(), "localhost")

    override def get(keys: Seq[String]) = Future {
      val jedis = pool.getResource
      try {
        val result = jedis.mget(keys: _*).toSeq
        jedis.close()
        result
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
        jedis.close()
        keys
      } catch {
        case NonFatal(ex) =>
          jedis.close()
          throw ex
      }
    }
  }

}
