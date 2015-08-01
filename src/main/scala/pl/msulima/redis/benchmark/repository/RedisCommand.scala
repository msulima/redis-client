package pl.msulima.redis.benchmark.repository

import io.netty.buffer.ByteBuf
import pl.msulima.redis.benchmark.repository.RedisParser.{MatchResult, Matcher, Payload}


object Integer {

  val matcher: Matcher = RedisParser.parseFragmentAndThen(Integer.apply, integer => {
    RedisParser.parseFragment(Bytes(2), _ => {
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
    part.readerIndex(part.readerIndex() + number.length)

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

  private def apply0(read: Int, left: Int, acc: Array[Byte])(part: Payload): MatchResult = {
    val toRead = Math.min(part.readableBytes(), left)
    part.readBytes(acc, read, toRead)

    if (left == toRead) {
      Left(acc)
    } else {
      Right(this.apply0(read + toRead, left - toRead, acc) _)
    }
  }

  def debug(array: Array[Byte]) = {
    array.map(x => {
      if (x.toChar.isControl) {
        "^"
      } else {
        x.toChar
      }
    }).mkString("\"", "", "\"")
  }
}

object NewLine {

  val matcher: Matcher = Bytes(2)
}

object BulkString {

  val matcher: Matcher = {
    RedisParser.parseFragment(Integer.matcher, x => {
      val length = x.asInstanceOf[Int]
      if (length == -1) {
        Left(null)
      } else {
        Right(RedisParser.parseFragmentAndThen(Bytes(length), string => {
          RedisParser.parseFragment(NewLine.matcher, _ => {
            Left(new String(string.asInstanceOf[Array[Byte]]))
          })
        }))
      }
    })
  }
}

object RedisArray {

  val matcher: Matcher = {
    RedisParser.parseFragment(Integer.matcher, x => {
      val length = x.asInstanceOf[Int]
      partMatcher(0, new Array[Any](length))
    })
  }

  private def partMatcher(read: Int, acc: Array[Any]): MatchResult = {
    if (read == acc.length) {
      Left(acc)
    } else {
      Right(RedisParser.parseFragment(RedisParser.matcher, x => {
        acc(read) = x
        partMatcher(read + 1, acc)
      }))
    }
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
        if (part.readableBytes() > 0) {
          then(result)(part)
        } else {
          Right(then(result))
        }
      case Right(m) =>
        val next = (nextPart: Payload) => parseFragmentAndThen(m.asInstanceOf[Matcher], then)(nextPart)
        if (part.readableBytes() > 0) {
          next(part)
        } else {
          Right(next)
        }
    }
  }

  def parseFragment(matcher: Matcher, then: Any => MatchResult): Matcher = (part: Payload) => {
    matcher(part) match {
      case Left(result) =>
        then(result)
      case Right(m) =>
        val next = (nextPart: Payload) => parseFragment(m.asInstanceOf[Matcher], then)(nextPart)
        if (part.readableBytes() > 0) {
          next(part)
        } else {
          Right(next)
        }
    }
  }

  def apply(part: Payload): MatchResult = {
    matcher(part)
  }

  val matcher: Matcher = {
    parseFragmentAndThen(Bytes(1), x => x.asInstanceOf[Array[Byte]](0) match {
      case Array =>
        RedisArray.matcher
      case BulkStringMarker =>
        BulkString.matcher
      case IntegerMarker =>
        Integer.matcher
    })
  }
}

