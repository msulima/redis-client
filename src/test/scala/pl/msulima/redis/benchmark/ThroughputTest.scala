package pl.msulima.redis.benchmark

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CountDownLatch, Executors}

import pl.msulima.redis.benchmark.jedis.JedisClient
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig, Pipeline}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

object ThroughputTest extends App {

  private val limit = 10000
  private val keys = (1 to limit).map(i => {
    i.toString.getBytes
  }).toArray
  private val items = (1 to limit).map(i => {
    (("." * 100) + i).getBytes
  }).toArray

  private val repeats = 5 * 1000 * 1000
  private val printPeriod = 1000 * 1000

  private implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(9))
  private val pool = new JedisPool(new JedisPoolConfig, "localhost")

  def test(batchSize: Int, repeats: Int): Unit = {
    val start = System.currentTimeMillis()
    val fs = for (j <- 1 to (repeats / batchSize).toInt) yield Future {
      val jedis: Jedis = pool.getResource
      try {
        val pipeline: Pipeline = jedis.pipelined
        var i = 0
        while (i < batchSize) {
          if ((i + j) % 10 == 0) {
            pipeline.set(keys(i), items(i))
          } else {
            pipeline.get(keys(i))
          }
          i = i + 1
        }
        Thread.sleep(1)
        pipeline.sync()
        if (j * batchSize % printPeriod == 0) {
          print(j * batchSize, start)
        }
      } finally {
        if (jedis != null) jedis.close()
      }
    }

    Await.ready(Future.sequence(fs), 10.minutes)
    print(repeats, start)
  }

  def testClient(batchSize: Int, repeats: Int): Unit = {
    val start = System.currentTimeMillis()

    val client = new JedisClient(batchSize)

    var j = 0
    val done = new AtomicInteger()
    val latch = new CountDownLatch(1)

    while (j < repeats) {
      val setCallback = new Runnable {
        override def run(): Unit = {
          val j = done.incrementAndGet()
          if (j == repeats) {
            latch.countDown()
          }
          if (j % printPeriod == 0) {
            print(j, start)
          }
        }
      }
      val getCallback = new java.util.function.Function[Array[Byte], Void] {
        override def apply(t: Array[Byte]): Void = {
          val j = done.incrementAndGet()
          if (j == repeats) {
            latch.countDown()
          }
          if (j % printPeriod == 0) {
            print(j, start)
          }
          null
        }
      }

      val k = j % limit

      if (j % 10 == 0) {
        client.set(keys(k), items(k), setCallback)
      } else {
        client.get(keys(k), getCallback)
      }
      j = j + 1
    }

    latch.await()
    print(repeats, start)
  }

  def suite(test: (Int, Int) => Unit): Unit = {
    test(1000, repeats)
    println("Warmup over")
    Seq(10, 20, 50, 100, 200, 500, 1000, 2000).foreach(batchSize => {
      test(batchSize, repeats)
      println(s"Done $batchSize")
    })
  }

  suite(testClient)
  suite(test)

  System.exit(0)

  def print(repeats: Int, start: Long): Unit = {
    val duration = System.currentTimeMillis() - start
    println((s"${repeats * 1000L / duration} /s", duration))
  }
}
