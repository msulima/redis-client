package pl.msulima.redis.benchmark.repository

import io.netty.buffer.ByteBuf
import pl.msulima.redis.benchmark.repository.RedisParser.{MatchResult, Matcher, Payload}


object Until {

  def apply(until: Char): Matcher = {
    apply0(Array(), until)
  }

  private def apply0(acc: Array[Byte], until: Char)(part: Payload): MatchResult = {
    val x = new Array[Byte](part.readableBytes())
    part.getBytes(part.readerIndex(), x)
    val number = x.takeWhile(_ != until)
    part.readerIndex(part.readerIndex() + number.length)

    if (part.readableBytes() < number.length) {
      Right(this.apply0(acc ++ number, until))
    } else {
      Left(acc ++ number)
    }
  }
}

object Integer {

  val matcher: Matcher = RedisParser.parseFragmentAndThen(Until('\r'), integer => {
    RedisParser.parseFragment(NewLine.matcher, _ => {
      val int = new Integer(new Predef.String(integer.asInstanceOf[Array[Byte]]).toInt)
      Left(int)
    })
  })
}

object SimpleString {

  val matcher: Matcher = RedisParser.parseFragmentAndThen(Until('\r'), string => {
    RedisParser.parseFragment(NewLine.matcher, _ => {
      Left(new String(string.asInstanceOf[Array[Byte]]))
    })
  })
}

object Error {

  val matcher: Matcher = RedisParser.parseFragmentAndThen(Until('\r'), string => {
    RedisParser.parseFragment(NewLine.matcher, _ => {
      Left(new RuntimeException(new String(string.asInstanceOf[Array[Byte]])))
    })
  })
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


  def debugResult(array: AnyRef) = {
    array match {
      case s: String =>
        s
      case arr: Array[_] =>
        arr.map(y => {
          debug(y.asInstanceOf[Array[Byte]])
        }).mkString("[", ", ", "]")
    }
  }

  def debug(array: Array[Byte]) = {
    if (array == null) {
      "<null>"
    } else {
      array.map(x => {
        val char = x.toChar
        if (char == '\r') {
          "\\r"
        } else if (char == '\n') {
          "\\n"
        } else if (char.isControl) {
          "^"
        } else if (char < 128) {
          char
        } else {
          "^"
        }
      }).mkString("\"", "", "\"")
    }
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
            Left(string)
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

  type MatchResult = Either[AnyRef, (Payload) => AnyRef]
  type Matcher = (Payload) => MatchResult
  type Payload = ByteBuf

  private val SimpleStringMarker = '+'
  private val ErrorMarker = '-'
  private val IntegerMarker = ':'
  private val BulkStringMarker = '$'
  private val Array = '*'

  def parseFragmentAndThen(matcher: Matcher, then: AnyRef => Matcher): Matcher = (part: Payload) => {
    matcher(part) match {
      case Left(result) if part.readableBytes() > 0 =>
        then(result)(part)
      case Left(result) =>
        Right(then(result))
      case Right(m) =>
        val next = (nextPart: Payload) => parseFragmentAndThen(m.asInstanceOf[Matcher], then)(nextPart)
        if (part.readableBytes() > 0) {
          next(part)
        } else {
          Right(next)
        }
    }
  }

  def parseFragment(matcher: Matcher, then: AnyRef => MatchResult): Matcher = (part: Payload) => {
    matcher(part) match {
      case Left(result) =>
        then(result) match {
          case Left(v) =>
            Left(v)
          case Right(m) =>
            m.asInstanceOf[Matcher](part)
        }
      case Right(m) =>
        val next = (nextPart: Payload) => parseFragment(m.asInstanceOf[Matcher], then)(nextPart)
        if (part.readableBytes() > 0) {
          next(part)
        } else {
          Right(next)
        }
    }
  }

  val matcher: Matcher = {
    parseFragmentAndThen(Bytes(1), x => {
      x.asInstanceOf[Array[Byte]](0) match {
        case Array =>
          RedisArray.matcher
        case BulkStringMarker =>
          BulkString.matcher
        case IntegerMarker =>
          Integer.matcher
        case SimpleStringMarker =>
          SimpleString.matcher
        case ErrorMarker =>
          Error.matcher
      }
    })
  }
}

