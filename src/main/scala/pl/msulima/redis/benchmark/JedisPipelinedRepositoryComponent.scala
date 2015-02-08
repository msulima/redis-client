package pl.msulima.redis.benchmark

import java.util.concurrent.ConcurrentLinkedQueue

import pl.msulima.redis.benchmark.PipeliningActor.{Get, Request, Set}
import redis.clients.jedis.{Jedis, Response}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}

trait JedisPipelinedRepositoryComponent {

  class JedisPipelinedRepository(implicit ec: ExecutionContext) extends Repository {

    private val requests = new ConcurrentLinkedQueue[(Promise[_], Request)]()

    override def get(keys: Seq[String]): Future[Seq[String]] = {
      val promisedStrings = Promise[Seq[String]]()
      requests.synchronized {
        requests.add(promisedStrings, Get(keys))
      }
      promisedStrings.future
    }

    override def set(keys: Seq[(String, String)]) = {
      val promisedStrings = Promise[Seq[(String, String)]]()
      requests.add(promisedStrings, Set(keys))
      promisedStrings.future
    }

    new Thread("redis") {
      val connection = new Jedis("localhost")

      override def run() = {
        while (true) {
          handleBatch()
        }
      }

      private def handleBatch() = {
        val elements = mutable.ListBuffer[(Promise[_], Request)]()

        Thread.sleep(10)
        requests.synchronized {
          requests.copyToBuffer(elements)
          requests.clear()
        }

        val jedis = connection.pipelined()
        val pipelined = elements.map {
          case (replyTo, Get(keys)) =>
            (replyTo, Get(keys), jedis.mget(keys: _*))
          case (replyTo, Set(keys)) =>
            (replyTo, Set(keys), jedis.mset(keys.flatMap(k => Seq(k._1, k._2)): _*))
        }
        jedis.sync()
        pipelined.foreach {
          case (promise, Get(_), response) =>
            promise.asInstanceOf[Promise[Seq[String]]].success(response.asInstanceOf[Response[java.util.List[String]]].get.toSeq)
          case (promise, Set(keys), _) =>
            promise.asInstanceOf[Promise[Seq[(String, String)]]].success(keys)
        }
      }
    }.start()
  }

}

