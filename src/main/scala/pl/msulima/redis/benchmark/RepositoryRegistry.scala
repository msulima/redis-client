package pl.msulima.redis.benchmark

import java.util.concurrent.Executors

import akka.actor.ActorSystem
import pl.msulima.redis.benchmark.repository._

import scala.concurrent.ExecutionContext

trait RepositoryRegistry
  extends JedisMultiGetRepositoryComponent
  with BrandoMultiGetRepositoryComponent
  with JedisAkkaPipelinedRepositoryComponent
  with JedisAkkaBatchRepositoryComponent
  with JedisPipelinedRepositoryComponent
  with NettyRepositoryComponent {

  implicit val system: ActorSystem
  implicit lazy val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(16))

  lazy val jedisMultiGetRepository = new JedisMultiGetRepository
  lazy val jedisAkkaPipelinedRepository = new JedisAkkaPipelinedRepository(system)
  lazy val jedisAkkaBatchRepository = new JedisAkkaBatchRepository(system)
  lazy val jedisPipelinedRepository: JedisPipelinedRepository = null
  lazy val brandoMultiGetRepository = new BrandoMultiGetRepository(system)
  lazy val nettyRepository = new NettyRepository
}
