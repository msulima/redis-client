package pl.msulima.redis.benchmark.repository

import java.util

import io.netty.buffer.ByteBuf
import org.apache.commons.io.output.ByteArrayOutputStream

case object ResponseNotReady

class EffectiveRedisParser extends Returns {

  private val SimpleStringMarker = '+'
  private val ErrorMarker = '-'
  private val IntegerMarker = ':'
  private val BulkStringMarker = '$'
  private val Array = '*'

  private type Step = ByteBuf => AnyRef

  protected val acc = new ByteArrayOutputStream()
  private val callStack = new util.Stack[Step]

  def reset(): Unit = {
    callStack.clear()
    callStack.push(readStart)
    acc.reset()
    read = 0
    left = 0
  }

  def read(buf: ByteBuf): AnyRef = {
    step(buf)
  }

  private def readStart(buf: ByteBuf): AnyRef = {
    if (buf.readableBytes() == 0) {
      ResponseNotReady
    } else {
      stepDown()
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
          RedisArray.matcher
      }
      step(buf)
    }
  }

  private def readToCR(part: ByteBuf): AnyRef = {
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

  private def readError(part: ByteBuf): AnyRef = {
    stepDown()
    callStack.push(returnException)
    callStack.push(readToCR)
    step(part)
  }

  private def readSimpleString(part: ByteBuf): AnyRef = {
    stepDown()
    callStack.push(returnString)
    callStack.push(readToCR)
    step(part)
  }

  private def readInteger(part: ByteBuf): AnyRef = {
    stepDown()
    callStack.push(returnInteger)
    callStack.push(readToCR)
    step(part)
  }

  private def readBulkString(part: ByteBuf): AnyRef = {
    stepDown()
    callStack.push(returnByteArray)
    callStack.push(readToCR)
    callStack.push(readAsMuchBytesAsInLastRead)
    callStack.push(readToCR)
    step(part)
  }

  private def readAsMuchBytesAsInLastRead(part: ByteBuf): AnyRef = {
    stepDown()
    acc.reset()
    val s = new Predef.String(acc.toByteArray)
    prepareToReadBytes(Integer.valueOf(s.dropRight(2)))
    step(part)
  }

  private var read: Int = 0
  private var left: Int = 0

  private def prepareToReadBytes(howMany: Int): Unit = {
    read = 0
    left = howMany
    callStack.push(readBytes)
  }

  private def readBytes(part: ByteBuf): AnyRef = {
    println(Bytes.debug(part))
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

  private def step(part: ByteBuf): AnyRef = {
    callStack.peek()(part)
  }

  private def stepDown(): Unit = {
    println("stepDown")
    callStack.pop()
  }
}

trait Returns {
  this: EffectiveRedisParser =>

  protected def returnString(part: ByteBuf): AnyRef = {
    val s = new Predef.String(acc.toByteArray)
    s.dropRight(2)
  }

  protected def returnByteArray(part: ByteBuf): AnyRef = {
    util.Arrays.copyOfRange(acc.toByteArray, 0, acc.size() - 2)
  }

  protected def returnException(part: ByteBuf): AnyRef = {
    val s = new Predef.String(acc.toByteArray)
    new RuntimeException(s.dropRight(2))
  }

  protected def returnInteger(part: ByteBuf): AnyRef = {
    val s = new Predef.String(acc.toByteArray)
    Integer.valueOf(s.dropRight(2))
  }

}