package pl.msulima.redis.benchmark

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CountDownLatch, Executors}

import pl.msulima.redis.benchmark.jedis.JedisClient
import redis.clients.jedis.{Jedis, JedisPool, Pipeline}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

object ThroughputTest extends App {

  private val limit = 1000000
  private val keys = (1 to limit).map(i => {
    i.toString.getBytes
  }).toArray
  private val items = (1 to limit).map(i => {
    (("." * 100) + i).getBytes
  }).toArray

  private val repeats = 5 * 1000 * 1000
  private val printPeriod = 200 * 1000
  private val SetRatio = 10

  private implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(9))
  private val pool = new JedisPool("rec-online1.rec-test.pl-kra-01.dc4.local", 6379)

  def test(batchSize: Int, repeats: Int): Unit = {
    val start = System.currentTimeMillis()
    val fs = for (j <- 1 to (repeats / batchSize)) yield Future {
      val jedis: Jedis = pool.getResource
      try {
        val pipeline: Pipeline = jedis.pipelined
        var i = 0
        while (i < batchSize) {
          val k = (i + (j - 1) * batchSize) % limit
          if ((i + j) % SetRatio == 0) {
            pipeline.set(keys(k), items(k))
          } else {
            pipeline.get(keys(k))
          }
          i = i + 1
        }
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

      if (j % SetRatio == 0) {
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
    testClient(100, repeats / 2)
    println("Warmup over")
    Seq(50, 100, 200, 10, 20, 500, 1000, 2000).foreach(batchSize => {
      println(s"Start $batchSize")
      testClient(batchSize, repeats)
    })
  }

  //
  //  val jedis: Jedis = pool.getResource
  //  if (jedis.dbSize() < keys.length) {
  //    println("Populate db")
  //    val pipeline = jedis.pipelined()
  //    keys.zip(items).foreach({
  //      case (key, item) =>
  //        pipeline.set(key, item)
  //    })
  //    pipeline.sync()
  //  }
  //  jedis.close()

  if (args.isEmpty) {
    suite(testClient)
    suite(test)
  } else {
    val batchSize = args(0).toInt
    testClient(batchSize, repeats / 2)
    println("Warmup over")
    println(s"Start $batchSize")
    testClient(batchSize, repeats)
  }

  System.exit(0)

  def print(repeats: Int, start: Long): Unit = {
    val duration = System.currentTimeMillis() - start
    println((s"${repeats * 1000L / duration}/s", duration))
  }
}
