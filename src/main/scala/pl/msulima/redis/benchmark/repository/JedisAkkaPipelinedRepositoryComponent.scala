package pl.msulima.redis.benchmark.repository

import java.util
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import pl.msulima.redis.benchmark.repository.PipeliningActor._
import redis.clients.jedis.{JedisPool, JedisPoolConfig, Response}

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

trait JedisAkkaPipelinedRepositoryComponent {

  class JedisAkkaPipelinedRepository(system: ActorSystem)(implicit ec: ExecutionContext) extends Repository {
    private val props = Props(new PipeliningActor)
    private val actor = system.actorOf(props)
    private implicit val timeout = Timeout(1, TimeUnit.SECONDS)

    override def mget(keys: Seq[String]): Future[Seq[Payload]] = (actor ? Get(keys)).mapTo[Seq[Payload]]

    override def mset(keys: Seq[(String, Payload)]): Future[Seq[(String, Payload)]] = (actor ? Set(keys)).mapTo[Seq[(String, Payload)]]
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
            (replyTo, Get(keys), jedis.mget(keys.map(_.getBytes): _*))
          case (replyTo, Set(keys)) =>
            (replyTo, Set(keys), jedis.mset(keys.flatMap(k => Seq(k._1.getBytes, k._2)): _*))
        }
        jedis.sync()
        pipelined.foreach {
          case (replyTo, Get(_), response) =>
            replyTo ! response.asInstanceOf[Response[util.List[Repository#Payload]]].get.toSeq.filterNot(_ == null)
          case (replyTo, PipeliningActor.Set(keys), _) =>
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

  case class Set(keys: Seq[(String, Repository#Payload)]) extends Request

  case object Tick

}

