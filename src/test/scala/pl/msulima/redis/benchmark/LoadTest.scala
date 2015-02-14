
package pl.msulima.redis.benchmark


import io.gatling.core.Predef._
import io.gatling.http.Predef._

import scala.concurrent.duration._

class LoadTest extends Simulation {

  val httpConf = http.baseURL("http://localhost:8080")

  //  val sut = "jedis/multi"
  //  val sut = "jedis/akka-pipelined"
  val sut = "jedis/pipelined"
  //  val sut = "brando/multi"

  private val Users = 200
  private val Duration = 60
  private val RPS = 100000
  private val SetRatio = 0.1
  private val GroupSize = 5
  private val PerUser = RPS * GroupSize / Users

  private val path = s"/$sut/random/$PerUser"
  private val getScenario = scenario("LoadTest mget")
    .exec(http(sut).get(path))
    .pause(0.milliseconds, 1.second)
  private val setScenario = scenario("LoadTest mset")
    .exec(http(sut).put(path))
    .pause(0.milliseconds, 1.second)

  val getUsers = Users * (1 - SetRatio)
  val setUsers = Users * SetRatio
  setUp(
    getScenario.inject(rampUsersPerSec(1) to getUsers during (Duration / 4), constantUsersPerSec(getUsers) during Duration),
    setScenario.inject(rampUsersPerSec(1) to setUsers during (Duration / 4), constantUsersPerSec(setUsers) during Duration)
  ).protocols(httpConf)

}
