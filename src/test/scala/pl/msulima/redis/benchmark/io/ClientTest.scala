package pl.msulima.redis.benchmark.io

import java.util.concurrent.ExecutionException

import org.scalatest.{FlatSpec, Matchers}

class ClientTest extends FlatSpec with Matchers {

  "client" should "read commands one after another" in {
    // given
    val client = createClient()

    // when
    client.ping().get() should be("PONG")
    client.ping("1").get() should be("1")
    client.ping("hello, it's me").get() should be("hello, it's me")
  }

  it should "run many simultaneously, but in order" in {
    // given
    val client = createClient()

    val key = "test 1".getBytes
    val value = "value of 1".getBytes

    // when
    client.del(key)
    val get1 = client.get(key)
    val set = client.set(key, value)
    val get2 = client.get(key)

    get1.get() should be(null)
    get2.get() should be(value)
    set.get() should be("OK")
  }

  it should "allow to expire keys" in {
    // given
    val client = createClient()

    val key = "test 2".getBytes
    val value = "value of 2".getBytes

    // when
    client.del(key)
    val set = client.setex(key, 1, value)
    val get1 = client.get(key)

    set.get() should be("OK")
    get1.get() should be(value)
    Thread.sleep(1000)
    val get2 = client.get(key)
    get2.get() should be(null)
  }

  it should "handle errors" in {
    // given
    val client = createClient()

    val key = "test 3".getBytes
    val value = "value of 3".getBytes

    // when
    val set = client.setex(key, -1, value)

    intercept[ExecutionException] {
      set.get() should be("OK")
    }
  }

  private def createClient(): SyncClient = {
    new SyncClient("localhost", 6379)
  }
}
