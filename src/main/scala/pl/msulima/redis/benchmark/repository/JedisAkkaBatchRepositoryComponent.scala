package pl.msulima.redis.benchmark.repository

import java.util
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{Executors, LinkedBlockingQueue, TimeUnit}

import akka.actor.{Actor, ActorSystem, Props}
import akka.util.Timeout
import pl.msulima.redis.benchmark.repository.PipeliningActor.{Get, Request, Set, Tick}
import redis.clients.jedis.{JedisPool, Response}

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}

trait JedisAkkaBatchRepositoryComponent {

  class JedisAkkaBatchRepository(system: ActorSystem)(implicit ec: ExecutionContext) extends Repository {
    private val props = Props(new PipeliningActor)
    private val actor = system.actorOf(props)
    private implicit val timeout = Timeout(1, TimeUnit.SECONDS)

    override def mget(keys: Seq[String]): Future[Seq[Payload]] = {
      val promisedStrings = Promise[Seq[Payload]]()
      actor !(promisedStrings, Get(keys))
      promisedStrings.future
    }

    override def mset(keys: Seq[(String, Payload)]) = {
      val promisedStrings = Promise[Seq[(String, Payload)]]()
      actor !(promisedStrings, Set(keys))
      promisedStrings.future
    }
  }

  class PipeliningActor extends Actor {

    private val pool = new JedisPool("localhost")

    private val requests = new util.concurrent.ConcurrentLinkedQueue[(Promise[_], Request)]
    private implicit val ec = context.dispatcher

    override def preStart() = {
      context.system.scheduler.schedule(0.milliseconds, 20.milliseconds, self, Tick)
    }

    private val redisEc = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10))
    private val queueSize = new AtomicInteger(0)
    val batchSize = new AtomicInteger(500)
    val exceeded = new AtomicInteger(0)

    val decThreshold = 5
    val incThreshold = 15

    override def receive: Receive = {
      case request: (Promise[_], Request) =>
        requests.add(request)
        if (requests.size == batchSize.get) {
          runQuery()
        }
      case Tick =>
        runQuery()
        if (exceeded.get() > 10) {
          exceeded.set(10)
        } else if (exceeded.get() < -10) {
          exceeded.set(-10)
        }
    }

    private val queryQueue = new LinkedBlockingQueue[Vector[(Promise[_], Request)]]()
    private val lock = new ReentrantLock()

    private def runQuery() = {
      val requestsCopy = Vector.empty ++ requests

      if (requestsCopy.size > 0) {
        requests.clear()
        queryQueue.add(requestsCopy)

        val currentQueueSize = queueSize.getAndIncrement
        if (currentQueueSize > incThreshold) {
          if (exceeded.incrementAndGet() > 10 && batchSize.get < 5000) {
            val newBatchSize = batchSize.addAndGet(10)
            println((" " * (currentQueueSize / 3)) + currentQueueSize, newBatchSize)
            exceeded.set(0)
          }
        } else if (currentQueueSize < decThreshold && batchSize.get > 50) {
          if (exceeded.decrementAndGet() < -10) {
            val newBatchSize = batchSize.addAndGet(-10)
            println((" " * (currentQueueSize / 3)) + currentQueueSize, newBatchSize)
            exceeded.set(0)
          }
        }
        redisEc.execute(new RedisThread)
      }
    }

    class RedisThread extends Runnable {
      override def run(): Unit = {
        lock.lock()
        val (p, conn) = try {
          val requestsCopy = queryQueue.poll()

          val connection = pool.getResource
          val jedis = connection.pipelined()
          val pipelined = requestsCopy.map {
            case (replyTo, Get(keys)) =>
              (replyTo, Get(keys), jedis.mget(keys.map(_.getBytes): _*))
            case (replyTo, Set(keys)) =>
              (replyTo, Set(keys), jedis.mset(keys.flatMap(k => Seq(k._1.getBytes, k._2)): _*))
          }
          jedis.sync()
          (pipelined, connection)
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
        queueSize.decrementAndGet()
        conn.close()
      }
    }

  }

}
