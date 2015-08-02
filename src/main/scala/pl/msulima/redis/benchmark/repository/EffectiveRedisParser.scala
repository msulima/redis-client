package pl.msulima.redis.benchmark.repository

import java.util

import io.netty.buffer.ByteBuf
import org.apache.commons.io.output.ByteArrayOutputStream

case object ResponseNotReady

class EffectiveRedisParser extends Returns with BytesParser with ArraysReader {

  protected val SimpleStringMarker = '+'
  protected val ErrorMarker = '-'
  protected val IntegerMarker = ':'
  protected val BulkStringMarker = '$'
  protected val Array = '*'

  private type Step = ByteBuf => AnyRef

  protected val acc = new ByteArrayOutputStream()
  protected var toReturn: AnyRef = null
  protected val callStack = new util.Stack[Step]

  def reset(): Unit = {
    callStack.clear()
    callStack.push(readStart)
    acc.reset()
  }

  reset()

  def parse(buf: ByteBuf): AnyRef = {
    step(buf)
  }

  private def readStart(buf: ByteBuf): AnyRef = {
    if (buf.readableBytes() == 0) {
      ResponseNotReady
    } else {
      stepDown()
      callStack.push(returnToUser)
      buf.readByte() match {
        case SimpleStringMarker =>
          callStack.push(readSimpleString)
        case ErrorMarker =>
          callStack.push(readError)
        case IntegerMarker =>
          callStack.push(readInteger)
        case BulkStringMarker =>
          callStack.push(readBulkString)
        case Array =>
          callStack.push(readArray)
      }
      step(buf)
    }
  }

  protected def readToCR(part: ByteBuf): AnyRef = {
    var x = 0
    while (x != '\r' && part.readableBytes() > 0) {
      x = part.readByte()
      acc.write(x)

      if (x == '\r') {
        stepDown()
        prepareToReadBytes(1)
        return step(part)
      }
    }
    ResponseNotReady
  }

  protected def readError(part: ByteBuf): AnyRef = {
    stepDown()
    acc.reset()
    callStack.push(returnException)
    callStack.push(readToCR)
    step(part)
  }

  protected def readSimpleString(part: ByteBuf): AnyRef = {
    stepDown()
    acc.reset()
    callStack.push(returnString)
    callStack.push(readToCR)
    step(part)
  }

  protected def readInteger(part: ByteBuf): AnyRef = {
    stepDown()
    acc.reset()
    callStack.push(returnInteger)
    callStack.push(readToCR)
    step(part)
  }

  protected def readBulkString(part: ByteBuf): AnyRef = {
    stepDown()
    acc.reset()
    callStack.push(readAsMuchBytesAsInLastRead)
    callStack.push(readToCR)
    step(part)
  }

  private def readAsMuchBytesAsInLastRead(part: ByteBuf): AnyRef = {
    stepDown()
    val s = new Predef.String(acc.toByteArray)
    val bytes = Integer.valueOf(s.dropRight(2))

    if (bytes == -1) {
      callStack.push(returnNil)
    } else {
      callStack.push(returnByteArray)
      callStack.push(readToCR)
      prepareToReadBytes(bytes)
      acc.reset()
    }
    step(part)
  }

  protected def step(part: ByteBuf): AnyRef = {
    callStack.peek()(part)
  }

  protected def stepDown(): Unit = {
    callStack.pop()
  }
}

trait BytesParser {
  this: EffectiveRedisParser =>

  private var read: Int = 0
  private var left: Int = 0

  protected def prepareToReadBytes(howMany: Int): Unit = {
    read = 0
    left = howMany
    callStack.push(readBytes)
  }

  private def readBytes(part: ByteBuf): AnyRef = {
    val toRead = Math.min(part.readableBytes(), left)
    part.readBytes(acc, toRead)

    if (left == toRead) {
      stepDown()
      step(part)
    } else {
      left = left - toRead
      read = read + toRead
      ResponseNotReady
    }
  }
}

trait Returns {
  this: EffectiveRedisParser =>

  protected def returnToUser(part: ByteBuf): AnyRef = {
    toReturn
  }

  protected def returnString(part: ByteBuf): AnyRef = {
    val s = new Predef.String(acc.toByteArray)
    toReturn = s.dropRight(2)
    stepDown()
    step(part)
  }

  protected def returnByteArray(part: ByteBuf): AnyRef = {
    toReturn = util.Arrays.copyOfRange(acc.toByteArray, 0, acc.size() - 2)
    stepDown()
    step(part)
  }

  protected def returnNil(part: ByteBuf): AnyRef = {
    toReturn = null
    stepDown()
    step(part)
  }

  protected def returnException(part: ByteBuf): AnyRef = {
    val s = new Predef.String(acc.toByteArray)
    toReturn = new RuntimeException(s.dropRight(2))
    stepDown()
    step(part)
  }

  protected def returnInteger(part: ByteBuf): AnyRef = {
    val s = new Predef.String(acc.toByteArray)
    toReturn = Integer.valueOf(s.dropRight(2))
    stepDown()
    step(part)
  }
}

trait ArraysReader {
  this: EffectiveRedisParser =>

  private var length = 0
  private val result = new util.ArrayList[AnyRef]()

  protected def readArray(buf: ByteBuf): AnyRef = {
    stepDown()
    result.clear()
    callStack.push(parseLength)
    callStack.push(readToCR)
    step(buf)
  }

  private def parseLength(buf: ByteBuf): AnyRef = {
    stepDown()
    val s = new Predef.String(acc.toByteArray)
    length = Integer.valueOf(s.dropRight(2))
    acc.reset()
    callStack.push(readNext)
    step(buf)
  }

  private def readNext(buf: ByteBuf): AnyRef = {
    if (result.size() == length) {
      toReturn = result.toArray
      stepDown()
      step(buf)
    } else {
      if (buf.readableBytes() == 0) {
        ResponseNotReady
      } else {
        stepDown()
        callStack.push(keepResult)
        buf.readByte() match {
          case SimpleStringMarker =>
            callStack.push(readSimpleString)
          case ErrorMarker =>
            callStack.push(readError)
          case IntegerMarker =>
            callStack.push(readInteger)
          case BulkStringMarker =>
            callStack.push(readBulkString)
        }
        step(buf)
      }
    }
  }

  private def keepResult(buf: ByteBuf): AnyRef = {
    stepDown()
    result.add(toReturn)
    callStack.push(readNext)
    step(buf)
  }
}