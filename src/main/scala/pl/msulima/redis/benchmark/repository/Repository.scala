package pl.msulima.redis.benchmark.repository

import scala.concurrent.Future

trait Repository {

  type Payload = Array[Byte]

  def mget(keys: Seq[String]): Future[Seq[Payload]]

  def mset(keys: Seq[(String, Payload)]): Future[Seq[(String, Payload)]]
}
