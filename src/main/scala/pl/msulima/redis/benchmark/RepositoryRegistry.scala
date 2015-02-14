package pl.msulima.redis.benchmark

import akka.actor.ActorSystem
import pl.msulima.redis.benchmark.repository.{BrandoMultiGetRepositoryComponent, JedisAkkaPipelinedRepositoryComponent, JedisMultiGetRepositoryComponent, JedisPipelinedRepositoryComponent}

trait RepositoryRegistry
  extends JedisMultiGetRepositoryComponent
  with BrandoMultiGetRepositoryComponent
  with JedisAkkaPipelinedRepositoryComponent
  with JedisPipelinedRepositoryComponent {

  implicit val system: ActorSystem
  private implicit lazy val ec = system.dispatcher

  lazy val jedisMultiGetRepository = new JedisMultiGetRepository
  lazy val jedisAkkaPipelinedRepository = new JedisAkkaPipelinedRepository(system)
  lazy val jedisPipelinedRepository = new JedisPipelinedRepository
  lazy val brandoMultiGetRepository = new BrandoMultiGetRepository(system)
}
