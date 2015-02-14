package pl.msulima.redis.benchmark.repository

import java.util
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import pl.msulima.redis.benchmark.repository.PipeliningActor.{Get, Request, Set, Tick}
import redis.clients.jedis.{JedisPool, JedisPoolConfig, Response}

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

trait JedisAkkaPipelinedRepositoryComponent {

  class JedisAkkaPipelinedRepository(system: ActorSystem)(implicit ec: ExecutionContext) extends Repository {
    private val props = Props(new PipeliningActor)
    private val actor = system.actorOf(props)
    private implicit val timeout = Timeout(1, TimeUnit.SECONDS)

    override def get(keys: Seq[String]): Future[Seq[String]] = (actor ? Get(keys)).mapTo[Seq[String]]

    override def set(keys: Seq[(String, String)]): Future[Seq[(String, String)]] = (actor ? Set(keys)).mapTo[Seq[(String, String)]]
  }

  class PipeliningActor extends Actor {

    private val pool = new JedisPool(new JedisPoolConfig(), "localhost")

    private val requests = new util.concurrent.ConcurrentLinkedQueue[(ActorRef, Request)]
    private implicit val ec = context.dispatcher

    override def preStart() = {
      context.system.scheduler.schedule(0.milliseconds, 10.milliseconds, self, Tick)
    }

    override def receive: Receive = {
      case request: Request =>
        requests.add(sender() -> request)
        if (requests.size == 1000) {
          self ! Tick
        }
      case Tick =>
        val connection = pool.getResource
        val jedis = connection.pipelined()
        val pipelined = requests.map {
          case (replyTo, Get(keys)) =>
            (replyTo, Get(keys), jedis.mget(keys: _*))
          case (replyTo, Set(keys)) =>
            (replyTo, Set(keys), jedis.mset(keys.flatMap(k => Seq(k._1, k._2)): _*))
        }
        jedis.sync()
        pipelined.foreach {
          case (replyTo, Get(_), response) =>
            replyTo ! response.asInstanceOf[Response[util.List[String]]].get.toSeq
          case (replyTo, Set(keys), _) =>
            replyTo ! keys
        }
        requests.clear()
        connection.close()
    }
  }

}

object PipeliningActor {

  sealed trait Request

  case class Get(keys: Seq[String]) extends Request

  case class Set(keys: Seq[(String, String)]) extends Request

  case object Tick

}

