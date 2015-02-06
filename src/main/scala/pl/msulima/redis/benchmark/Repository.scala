package pl.msulima.redis.benchmark

import scala.concurrent.Future

trait Repository {

  def get(keys: Seq[String]): Future[Seq[String]]

  def set(keys: Seq[(String, String)]): Future[Seq[(String, String)]]
}
