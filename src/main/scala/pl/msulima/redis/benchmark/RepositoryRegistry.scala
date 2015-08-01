package pl.msulima.redis.benchmark

import akka.actor.ActorSystem
import pl.msulima.redis.benchmark.repository._

trait RepositoryRegistry
  extends JedisMultiGetRepositoryComponent
  with BrandoMultiGetRepositoryComponent
  with JedisAkkaPipelinedRepositoryComponent
  with JedisAkkaBatchRepositoryComponent
  with JedisPipelinedRepositoryComponent
  with NettyRepositoryComponent {

  implicit val system: ActorSystem
  private implicit lazy val ec = system.dispatcher

  lazy val jedisMultiGetRepository = new JedisMultiGetRepository
  lazy val jedisAkkaPipelinedRepository = new JedisAkkaPipelinedRepository(system)
  lazy val jedisAkkaBatchRepository = new JedisAkkaBatchRepository(system)
  lazy val jedisPipelinedRepository = new JedisPipelinedRepository
  lazy val brandoMultiGetRepository = new BrandoMultiGetRepository(system)
  lazy val nettyRepository = new NettyRepository
}
