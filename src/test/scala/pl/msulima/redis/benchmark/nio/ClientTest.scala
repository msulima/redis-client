package pl.msulima.redis.benchmark.nio

import org.scalatest.{FlatSpec, Matchers}

class ClientTest extends FlatSpec with Matchers {

  "client" should "read commands one after another" in {
    // given
    val client = new Client

    // when
    client.ping().get() should be("PONG")
    client.ping("1").get() should be("1")
    client.ping("hello, it's me").get() should be("hello, it's me")
  }

  it should "run many simultaneously, but in order" in {
    // given
    val client = new Client

    val key = "test 1".getBytes
    val value = "value of 1".getBytes

    // when
    val rm = client.del(key)
    val get1 = client.get(key)
    val set = client.set(key, value)
    val get2 = client.get(key)

    get1.get() should be(null)
    get2.get() should be(value)
  }
}
