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
    RedisParser.parseFragment(Bytes(1), _ => {
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

    if (part.readableBytes() < number.length) {
      Right(this.apply0(acc ++ number))
    } else {
      val int = new Predef.String(acc ++ number).toInt
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
      println(s"Left(${acc.mkString})")
      Left(acc)
    } else {
      println(s"Right(this.apply0($read + $toRead, $left - $toRead, ${acc.mkString("[", "x", "]")}) _)")
      Right(this.apply0(read + toRead, left - toRead, acc) _)
    }
  }
}

object NewLine {

  val matcher: Matcher = Bytes(2)
}

object BulkString {

  val matcher: Matcher = {
    RedisParser.parseFragmentAndThen(Integer.apply, length => {
      RedisParser.parseFragmentAndThen(NewLine.matcher, _ => {
        RedisParser.parseFragmentAndThen(Bytes(length.asInstanceOf[Int]), string => {
          RedisParser.parseFragment(NewLine.matcher, _ => {
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
        println(s"case Left($result) =>")
        Right(then(result))
      case Right(m) =>
        println("case Right(m) =>")
        val next = (nextPart: Payload) => parseFragmentAndThen(m.asInstanceOf[Matcher], then)(nextPart)
        Right(next)
    }
  }

  def parseFragment(matcher: Matcher, then: Any => MatchResult): Matcher = (part: Payload) => {
    matcher(part) match {
      case Left(result) =>
        println(s"x case Left($result) =>")
        then(result)
      case Right(m) =>
        println("x case Right(m) =>")
        val next = (nextPart: Payload) => parseFragment(m.asInstanceOf[Matcher], then)(nextPart)
        Right(next)
    }
  }

  def apply(part: Payload): MatchResult = {
    matcher(part)
  }

  val matcher: Matcher = {
    parseFragmentAndThen(Bytes(1), x => x.asInstanceOf[Array[Byte]](0) match {
      case BulkStringMarker =>
        BulkString.matcher
      case IntegerMarker =>
        Integer.matcher
    })
  }
}

