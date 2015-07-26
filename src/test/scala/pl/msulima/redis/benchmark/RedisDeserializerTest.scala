package pl.msulima.redis.benchmark

import java.util

import org.scalatest.{FlatSpec, Matchers}
import pl.msulima.redis.benchmark.repository.RedisDeserializer
import pl.msulima.redis.benchmark.repository.RedisDeserializer.Matcher

class RedisDeserializerTest extends FlatSpec with Matchers {

  "deserializer" should "parse arrays" in {
    // given
    val response = """*3
                     |$3
                     |123
                     |:456
                     |$3
                     |789
                     | """.stripMargin.split("\n")

    // when
    val q = new util.LinkedList[String]()
    q.addAll(util.Arrays.asList(response: _*))
    val result = RedisDeserializer(q)

    // then
    result should be(Seq("123", 456, "789"))
  }

  it should "be asynchronous" in {
    // given
    val response = """*4
                     |$3
                     |123
                     |:456
                     |$-1
                     |$3
                     |789""".stripMargin.split("\n").toSeq

    // when
    def run(f: Matcher, l: Seq[String]): Any = {
      f(l.head) match {
        case Left(v) =>
          v
        case Right(nextF) =>
          run(nextF.asInstanceOf[Matcher], l.tail)
      }
    }
    val result = run(RedisDeserializer.dafuq, response)

    // then
    result should be(Seq("123", 456, null, "789"))
  }
}
