package pl.msulima.redis.benchmark.repository

import io.netty.buffer.ByteBuf
import pl.msulima.redis.benchmark.repository.RedisParser.{MatchResult, Matcher, Payload}


object RedisSingleInt {

  def apply(i: Int) = s":$i"

  def unapply(s: String): Option[Int] = {
    if (s.charAt(0) == ':') {
      Some(s.substring(1).toInt)
    } else {
      None
    }
  }
}

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

object RedisStringLength {

  def apply(s: Int) = s"$$$s"

  def unapply(s: String): Option[Int] = {
    if (s.charAt(0) == '$') {
      Some(s.substring(1).toInt)
    } else {
      None
    }
  }
}

object RedisArrayLength {

  def apply(s: Int) = s"*$s"

  def unapply(s: String): Option[Int] = {
    if (s.charAt(0) == '*') {
      Some(s.substring(1).toInt)
    } else {
      None
    }
  }
}

object RedisArray {

  def apply(length: Int): Matcher = {
    apply0(length, List.empty)
  }

  private def apply0(length: Int, acc: Seq[Any])(part: Payload): MatchResult = {

    def step(v: Any): MatchResult = {
      val nextAcc = acc :+ v
      if (nextAcc.size == length) {
        Left(nextAcc)
      } else {
        Right(apply0(length, nextAcc))
      }
    }

    part match {
      case RedisNil(value) =>
        step(null)
      case RedisSingleInt(value) =>
        step(value)
      case RedisStringLength(s) =>
        Right(next => step(next))
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
    val x = Array[Byte]()
    part.getBytes(part.readerIndex(), x)
    val number = acc ++ x.takeWhile(_ != '\r')
    part.readerIndex(part.readerIndex() + x.length)

    if (part.readableBytes == number.length) {
      Right(this.apply0(number))
    } else {
      val int = new Predef.String(number).toInt
      Left(int)
    }
  }
}

object Chars {

  def apply(length: Int)(part: Payload): MatchResult = {
    if (part.readableBytes < length) {
      Right(this.apply(length - part.readableBytes) _)
    } else {
      // todo return content
      Left(null)
    }
  }
}

object NewLine {

  def apply(part: Payload): MatchResult = {
    Chars(2)(part)
  }
}

object BulkString {

  val matcher: Matcher = {
    RedisParser.parseFragmentAndThen(Integer.apply, length => {
      RedisParser.parseFragmentAndThen(NewLine.apply, _ => {
        RedisParser.parseFragmentAndThen(Chars(length.asInstanceOf[Int]), string => {
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
        Left(then(result))
      case Right(m) =>
        val next = (nextPart: Payload) => parseFragmentAndThen(m.asInstanceOf[Matcher], then)(nextPart)
        Right(next)
    }
  }

  def apply(part: Payload): MatchResult = {
    parseFragmentAndThen(Chars(1), {
      case BulkStringMarker =>
        BulkString.matcher
      case IntegerMarker =>
        Integer.matcher
    })(part)
  }
}
