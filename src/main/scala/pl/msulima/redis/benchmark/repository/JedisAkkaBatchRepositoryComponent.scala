package pl.msulima.redis.benchmark.repository

import java.util
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{ConcurrentLinkedQueue, Executors, LinkedBlockingQueue, TimeUnit}
import java.util.{Timer, TimerTask}

import akka.actor.ActorSystem
import akka.util.Timeout
import pl.msulima.redis.benchmark.repository.PipeliningActor.{Get, Request, Set}
import redis.clients.jedis.{JedisPool, Pipeline, Response}

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContext, Future, Promise}

trait JedisAkkaBatchRepositoryComponent {

  class JedisAkkaBatchRepository(system: ActorSystem)(implicit ec: ExecutionContext) extends Repository {
    private implicit val timeout = Timeout(1, TimeUnit.SECONDS)
    private val requests = new ConcurrentLinkedQueue[(Promise[_], Request)]()
    private val batches = new LinkedBlockingQueue[util.LinkedList[(Promise[_], Request)]]()

    private val sleepTime = 10
    private val batchSize = 5000
    private val pool = new JedisPool("localhost")
    private val lock = new ReentrantLock()

    private val redisEc = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(5))
    private val redisSchedulerEc = new Timer()

    override def mget(keys: Seq[String]): Future[Seq[Payload]] = {
      val promisedStrings = Promise[Seq[Payload]]()
      requests.offer(promisedStrings -> Get(keys))
      promisedStrings.future
    }

    override def mset(keys: Seq[(String, Payload)]) = {
      val promisedStrings = Promise[Seq[(String, Payload)]]()
      requests.offer(promisedStrings -> Set(keys))
      promisedStrings.future
    }

    class Batcher extends TimerTask {
      override def run(): Unit = {
        readElements()
      }

      @tailrec
      private def readElements(): Unit = {
        val elements = new util.LinkedList[(Promise[_], Request)]()
        var element = requests.poll()
        while (element != null && elements.size() < batchSize) {
          elements += element
          element = requests.poll()
        }

        if (elements.size() == batchSize) {
          if (element != null) {
            elements += element
          }
          print(".")
          batches.offer(elements)
          readElements()
        } else {
          if (elements.size() > 0) {
            batches.offer(elements)
          }
          redisSchedulerEc.schedule(new Batcher, sleepTime)
        }
      }
    }

    class RedisThread extends Runnable {
      override def run(): Unit = {
        while (true) {
          lock.lock()
          val p = try {
            val requestsCopy = batches.take()

            val connection = pool.getResource
            val jedis = connection.pipelined()
            val pipelined = makeRequest(jedis, requestsCopy)
            jedis.sync()
            connection.close()
            pipelined
          } finally {
            lock.unlock()
          }

          p.foreach {
            case (promise, Get(_), response) =>
              val values = response.asInstanceOf[Response[util.List[Array[Byte]]]].get.toSeq.filterNot(_ == null)
              promise.asInstanceOf[Promise[Seq[Array[Byte]]]].success(values)
            case (promise, Set(keys), _) =>
              promise.asInstanceOf[Promise[Seq[(String, Array[Byte])]]].success(keys)
          }
        }
      }

      private def makeRequest(jedis: Pipeline, elements: util.List[(Promise[_], Request)]) = {
        elements.map {
          case (replyTo, Get(keys)) =>
            (replyTo, Get(keys), jedis.mget(keys.map(_.getBytes): _*))
          case (replyTo, Set(keys)) =>
            (replyTo, Set(keys), jedis.mset(keys.flatMap(k => Seq(k._1.getBytes, k._2)): _*))
        }
      }
    }

    for (i <- 1 to 5) {
      val thread = new Thread(new RedisThread)
      thread.setPriority(10)
      thread.start()
    }
    redisSchedulerEc.schedule(new Batcher, 10)
  }

}
