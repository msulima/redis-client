package pl.msulima.redis.benchmark.repository

import java.util
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import pl.msulima.redis.benchmark.repository.PipeliningActor.{Get, Request, Set}
import redis.clients.jedis.{Jedis, Response}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}

trait JedisPipelinedRepositoryComponent {

  class JedisPipelinedRepository(implicit ec: ExecutionContext) extends Repository {

    private val requests = new LinkedBlockingQueue[(Promise[_], Request)]()

    override def mget(keys: Seq[String]): Future[Seq[Payload]] = {
      val promisedStrings = Promise[Seq[Payload]]()
      requests.synchronized {
        requests.add(promisedStrings, Get(keys))
      }
      promisedStrings.future
    }

    override def mset(keys: Seq[(String, Payload)]) = {
      val promisedStrings = Promise[Seq[(String, Payload)]]()
      requests.synchronized {
        requests.add(promisedStrings, Set(keys))
      }
      promisedStrings.future
    }

    private val semaphore = new ReentrantLock()

    class RedisThread(id: Int) extends Thread(s"redis-$id") {
      private val connection = new Jedis("localhost")

      override def run() = {
        while (true) {
          handleBatch()
        }
      }

      private def handleBatch() = {
        val elements = mutable.ListBuffer[(Promise[_], Request)]()

        semaphore.lock()
        var element = requests.poll(10, TimeUnit.MILLISECONDS)
        if (element != null) {
          elements += element
          requests.synchronized {
            if (requests.size() > 1000) {
              println(id, requests.size())
            }
            requests.copyToBuffer(elements)
            requests.clear()
          }
        }

        val jedis = connection.pipelined()
        val pipelined = elements.map {
          case (replyTo, Get(keys)) =>
            (replyTo, Get(keys), jedis.mget(keys.map(_.getBytes): _*))
          case (replyTo, Set(keys)) =>
            (replyTo, Set(keys), jedis.mset(keys.flatMap(k => Seq(k._1.getBytes, k._2)): _*))
        }
        jedis.sync()
        semaphore.unlock()
        pipelined.foreach {
          case (promise, Get(_), response) =>
            val values = response.asInstanceOf[Response[util.List[Payload]]].get.toSeq.filterNot(_ == null)
            promise.asInstanceOf[Promise[Seq[Payload]]].success(values)
          case (promise, Set(keys), _) =>
            promise.asInstanceOf[Promise[Seq[(String, Payload)]]].success(keys)
        }
      }
    }

    new RedisThread(1).start()
    new RedisThread(2).start()
    new RedisThread(3).start()
    new RedisThread(4).start()
  }

}

