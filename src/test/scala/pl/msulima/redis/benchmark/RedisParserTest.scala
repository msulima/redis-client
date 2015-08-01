package pl.msulima.redis.benchmark

import io.netty.buffer.Unpooled
import org.scalatest.{FlatSpec, Matchers}
import pl.msulima.redis.benchmark.repository.{Bytes, RedisParser}
import pl.msulima.redis.benchmark.repository.RedisParser.{Matcher, Payload}

class RedisParserTest extends FlatSpec with Matchers {

  "deserializer" should "be asynchronous" in {
    // given
    val response = """*4
                     |$3
                     |123
                     |:456
                     |$-1
                     |$3
                     |789""".stripMargin.split("\n").toSeq.map(x => (x + "\r\n").getBytes)

    // when
    val result = run(response)

    // then
    result should be(Seq("123", 456, null, "789"))
  }

  it should "handle integers" in {
    // given
    val response = ":100\r\n"

    // when
    val result = run(response)

    // then
    result should be(100)
  }

  it should "handle binary strings" in {
    // given
    val response = "$8\r\n123\r\n678\r\n"

    // when
    val result = run(response)

    // then
    result should be("123\r\n678")
  }

  private def run(parts: String): Any = {
    run(parts.getBytes.toSeq.map(x => Array(x)))
  }

  private def run(parts: Seq[Array[Byte]]): Any = {
    println(parts.map(Bytes.debug))
    run0(RedisParser.matcher, parts.map(x => Unpooled.copiedBuffer(x)))
  }

  def run0(f: Matcher, l: Seq[Payload]): Any = {
    f(l.head) match {
      case Left(v) =>
        v
      case Right(nextF) =>
        run0(nextF.asInstanceOf[Matcher], l.tail)
    }
  }
}
