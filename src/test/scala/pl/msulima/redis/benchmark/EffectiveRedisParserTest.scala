package pl.msulima.redis.benchmark

import io.netty.buffer.Unpooled
import org.scalatest.{FlatSpec, Matchers}
import pl.msulima.redis.benchmark.repository.RedisParser.Payload
import pl.msulima.redis.benchmark.repository.{EffectiveRedisParser, ResponseNotReady}

class EffectiveRedisParserTest extends FlatSpec with Matchers {

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
    result should be(Array("123".getBytes, 456, null, "789".getBytes))
  }

  it should "handle integers" in {
    // given
    val response = ":100\r\n"

    // when
    val result = run(response)

    // then
    result should be(100)
  }

  it should "short strings" in {
    // given
    val response = "+OK\r\n"

    // when
    val result = run(response)

    // then
    result should be("OK")
  }

  it should "handle binary strings" in {
    // given
    val response = "$8\r\n123\r\n678\r\n"

    // when
    val result = run(response)

    // then
    result should be("123\r\n678".getBytes)
  }

  it should "parse errors" in {
    // given
    val response = "-Error message\r\n"

    // when
    val result = run(response)

    // then
    result.asInstanceOf[Throwable].getMessage should be("Error message")
  }

  private val parser = new EffectiveRedisParser

  private def run(parts: String): Any = {
    parser.reset()
    run(parts.getBytes.toSeq.map(x => Array(x)))
  }

  private def run(parts: Seq[Array[Byte]]): Any = {
    parser.reset()
    run0(parts.map(x => Unpooled.copiedBuffer(x)))
  }

  def run0(l: Seq[Payload]): Any = {
    parser.parse(l.head) match {
      case ResponseNotReady =>
        run0(l.tail)
      case v: AnyRef =>
        v
    }
  }
}
