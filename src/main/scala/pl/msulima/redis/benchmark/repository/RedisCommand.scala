package pl.msulima.redis.benchmark.repository

import io.netty.buffer.ByteBuf
import pl.msulima.redis.benchmark.repository.RedisParser.{MatchResult, Matcher, Payload}


object RedisNil {

  def apply(s: Unit) = "$-1"

  def unapply(s: String): Option[Unit] = {
    if (s == "$-1") {
      Some(())
    } else {
      None
    }
  }
}

object Integer {

  val matcher: Matcher = RedisParser.parseFragmentAndThen(Integer.apply, integer => {
    RedisParser.parseFragmentAndThen(NewLine.apply, _ => _ => {
      Left(integer)
    })
  })

  def apply(part: Payload): MatchResult = {
    apply0(Array())(part)
  }

  private def apply0(acc: Array[Byte])(part: Payload): MatchResult = {
    val x = new Array[Byte](part.readableBytes())
    part.getBytes(part.readerIndex(), x)
    val number = x.takeWhile(_ != '\r')
    part.readerIndex(part.readerIndex() + x.length)

    println(s"${part.readableBytes()}, ${number.length}")
    if (part.readableBytes() < number.length) {
      println("Right(this.apply0(number))")
      Right(this.apply0(acc ++ number))
    } else {
      val int = new Predef.String(acc ++ number).toInt
      println(s"val int = new Predef.String(number).toInt $int")
      Left(int)
    }
  }
}

object Bytes {

  def apply(length: Int)(part: Payload): MatchResult = {
    apply0(0, length, new Array[Byte](length))(part)
  }

  def apply0(read: Int, left: Int, acc: Array[Byte])(part: Payload): MatchResult = {
    val toRead = Math.min(part.readableBytes(), left)
    part.readBytes(acc, read, toRead)

    if (left == toRead) {
      println("Left(acc)")
      Left(acc)
    } else {
      println("Right(this.apply0(read + toRead, left - toRead, acc) _)")
      Right(this.apply0(read + toRead, left - toRead, acc) _)
    }
  }
}

object NewLine {

  def apply(part: Payload): MatchResult = {
    Bytes(2)(part)
  }
}

object BulkString {

  val matcher: Matcher = {
    RedisParser.parseFragmentAndThen(Integer.apply, length => {
      RedisParser.parseFragmentAndThen(NewLine.apply, _ => {
        RedisParser.parseFragmentAndThen(Bytes(length.asInstanceOf[Int]), string => {
          RedisParser.parseFragmentAndThen(NewLine.apply, _ => _ => {
            Left(string)
          })
        })
      })
    })
  }
}

object RedisParser {

  type MatchResult = Either[Any, (Payload) => Any]
  type Matcher = (Payload) => MatchResult
  type Payload = ByteBuf

  private val SimpleString = '+'
  private val Error = '-'
  private val IntegerMarker = ':'
  private val BulkStringMarker = '$'
  private val Array = '*'

  def parseFragmentAndThen(matcher: Matcher, then: Any => Matcher): Matcher = (part: Payload) => {
    matcher(part) match {
      case Left(result) =>
        println("case Left(result) =>")
        Right(then(result))
      case Right(m) =>
        println("case Right(m) =>")
        val next = (nextPart: Payload) => parseFragmentAndThen(m.asInstanceOf[Matcher], then)(nextPart)
        Right(next)
    }
  }

  def apply(part: Payload): MatchResult = {
    parseFragmentAndThen(Bytes(1), x => x.asInstanceOf[Array[Byte]](0) match {
      case BulkStringMarker =>
        BulkString.matcher
      case IntegerMarker =>
        println("Integer.matcher")
        Integer.matcher
    })(part)
  }
}
