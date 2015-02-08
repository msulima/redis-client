package pl.msulima.redis.benchmark


import io.gatling.core.Predef._
import io.gatling.http.Predef._

import scala.concurrent.duration._

class LoadTest extends Simulation {

  val httpConf = http.baseURL("http://localhost:8080")

  val scn = scenario("LoadTest")
    .exec(http("jedis/multi/random").get("/jedis/multi/random/500"))
    .pause(0.milliseconds, 1.second)

  private val Users = 200
  private val Duration = 60

  setUp(
    scn.inject(
      rampUsers(Users) over (Duration / 5),
      constantUsersPerSec(Users) during Duration
    )
  ).protocols(httpConf)

}
