
package pl.msulima.redis.benchmark


import io.gatling.core.Predef._
import io.gatling.http.Predef._

import scala.concurrent.duration._

class LoadTest extends Simulation {

  val httpConf = http.baseURL("http://localhost:8080")

  //  val sut = "jedis/multi"
  val sut = "jedis/akka-batch"
  //  val sut = "jedis/akka-pipelined"
//    val sut = "jedis/pipelined"
  //  val sut = "brando/multi"

  private val Users = 10000
  private val Duration = 120
  private val RedisPerSecond = 100000
  private val SetRatio = 0.1
  private val PauseDuration = 3
  private val GroupSize = 1
  private val PerRequest = (RedisPerSecond * GroupSize * PauseDuration) / Users

  private val path = s"/$sut/random/$PerRequest"
  private val repeats = Duration / PauseDuration
  private val getScenario = scenario(s"LoadTest mget $sut $PerRequest ${Users * PerRequest / PauseDuration}").repeat(repeats) {
    exec(http(sut).get(path).header("Accept-Encoding", "gzip")).pause(PauseDuration.seconds)
  }
  private val setScenario = scenario("LoadTest mset").repeat(repeats) {
    exec(http(sut).put(path).header("Accept-Encoding", "gzip")).pause(PauseDuration.seconds)
  }

  val setUsers = (Users * SetRatio).toInt
  val getUsers = Users - setUsers

  setUp(
    getScenario.inject(rampUsers(getUsers) over (Duration / 3)),
    setScenario.inject(rampUsers(setUsers) over (Duration / 3))
  ).protocols(httpConf).exponentialPauses

}
