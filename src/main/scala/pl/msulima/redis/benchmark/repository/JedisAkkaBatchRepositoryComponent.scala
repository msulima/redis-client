package pl.msulima.redis.benchmark.repository

import java.util
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, TimeUnit}

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import pl.msulima.redis.benchmark.repository.PipeliningActor.{Get, Request, Set, Tick}
import redis.clients.jedis.{Jedis, Response}

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

trait JedisAkkaBatchRepositoryComponent {

  class JedisAkkaBatchRepository(system: ActorSystem)(implicit ec: ExecutionContext) extends Repository {
    private val props = Props(new PipeliningActor)
    private val actor = system.actorOf(props)
    private implicit val timeout = Timeout(1, TimeUnit.SECONDS)

    override def mget(keys: Seq[String]): Future[Seq[Payload]] = (actor ? Get(keys)).mapTo[Seq[Payload]]

    override def mset(keys: Seq[(String, Payload)]): Future[Seq[(String, Payload)]] = (actor ? Set(keys)).mapTo[Seq[(String, Payload)]]
  }

  class PipeliningActor extends Actor {

    private val pool = new ThreadLocal[Jedis] {
      override def initialValue = new Jedis("localhost")
    }

    private val requests = new util.concurrent.ConcurrentLinkedQueue[(ActorRef, Request)]
    private implicit val ec = context.dispatcher

    override def preStart() = {
      context.system.scheduler.schedule(0.milliseconds, 20.milliseconds, self, Tick)
    }

    private val redisEc = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(5))
    private val queueSize = new AtomicInteger(0)

    override def receive: Receive = {
      case request: Request =>
        requests.add(sender() -> request)
        if (requests.size == 500) {
          runQuery()
        }
      case Tick =>
        runQuery()
    }

    private def runQuery() = {
      val requestsCopy = Vector.empty ++ requests
      val size = requestsCopy.size
      if (size > 1000) {
        println((" " * (size / 100)) + size)
      }
      requests.clear()

      if (size > 0) {
        val currentQueueSize = queueSize.getAndIncrement
        if (currentQueueSize > 20) {
          println((" " * (currentQueueSize / 3)) + currentQueueSize)
        }
        redisEc.execute(new Runnable {
          override def run(): Unit = {
            Thread.currentThread().setPriority(10)
            val connection = pool.get
            val jedis = connection.pipelined()
            val pipelined = requestsCopy.map {
              case (replyTo, Get(keys)) =>
                (replyTo, Get(keys), jedis.mget(keys.map(_.getBytes): _*))
              case (replyTo, Set(keys)) =>
                (replyTo, Set(keys), jedis.mset(keys.flatMap(k => Seq(k._1.getBytes, k._2)): _*))
            }
            jedis.sync()
            val result = pipelined.map {
              case (replyTo, Get(_), response) =>
                replyTo -> response.asInstanceOf[Response[util.List[Repository#Payload]]].get.toSeq.filterNot(_ == null)
              case (replyTo, Set(keys), _) =>
                replyTo -> keys
            }
            context.dispatcher.execute(new Runnable {
              override def run(): Unit = {
                result.foreach {
                  case (replyTo, response) =>
                    replyTo ! response
                }
              }
            })
            queueSize.decrementAndGet()
          }
        })
      }
    }
  }

}
