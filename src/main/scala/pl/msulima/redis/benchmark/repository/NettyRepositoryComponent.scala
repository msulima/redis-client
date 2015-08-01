package pl.msulima.redis.benchmark.repository

import scala.concurrent.{ExecutionContext, Future}

trait NettyRepositoryComponent {

  class NettyRepository(implicit val ec: ExecutionContext) extends Repository {

    private val client: RedisClient = new NettyRedisClient("localhost", 6379)

    override def mget(keys: Seq[String]): Future[Seq[Payload]] = {
      client.execute[AnyRef]("MGET", keys).map(x => {
        val arr = x.asInstanceOf[Array[AnyRef]]
        arr.flatMap(y => {
          Option(y.asInstanceOf[Array[Byte]])
        }).toSeq
      })
    }

    override def mset(keys: Seq[(String, Payload)]): Future[Seq[(String, Payload)]] = {
      val flatten: Seq[Payload] = keys.flatMap(x => Seq(x._1.getBytes, x._2))

      client.executeBinary[String]("MSET", flatten).map(_ => keys)
    }
  }

}
