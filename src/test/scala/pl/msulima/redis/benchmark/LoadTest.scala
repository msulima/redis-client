package pl.msulima.redis.benchmark


import io.gatling.core.Predef._
import io.gatling.http.Predef._

import scala.concurrent.duration._

class LoadTest extends Simulation {

  val httpConf = http.baseURL("http://localhost:8080")

  //  val sut = "jedis/multi"
  val sut = "jedis/pipelined"
  //  val sut = "brando/multi"

  private val Users = 200
  private val Duration = 60
  private val RPS = 100000
  private val GroupSize = 5
  private val PerUser = RPS * GroupSize / Users

  val scn = scenario("LoadTest")
    .exec(http(sut).get(s"/$sut/random/$PerUser"))
    .pause(0.milliseconds, 1.second)

  setUp(
    scn.inject(
      rampUsers(Users) over (Duration / 5),
      constantUsersPerSec(Users) during Duration
    )
  ).protocols(httpConf)

}
