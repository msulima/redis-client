package pl.msulima.redis.benchmark.nio

import org.scalatest.{FlatSpec, Matchers}

class ClientTest extends FlatSpec with Matchers {

  "client" should "run PING commands" in {
    // given
    val client = new Client

    // when
    client.ping().get() should be("PONG")
    client.ping("1").get() should be("1")
    client.ping("hello, it's me").get() should be("hello, it's me")
  }
}
