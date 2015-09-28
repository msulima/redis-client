package pl.msulima.redis.benchmark.repository

import java.io._
import java.net.Socket
import java.util.concurrent.{ConcurrentLinkedQueue, Executors}
import java.util.{Timer, TimerTask}

import io.netty.buffer.{ByteBuf, Unpooled}

import scala.concurrent.{Future, Promise}

class SyncClient extends RedisClient {

  private val socket = new Socket("localhost", 6379)

  private val os: OutputStream = new BufferedOutputStream(socket.getOutputStream)
  private val is: InputStream = new BufferedInputStream(socket.getInputStream)
  private val requests = new ConcurrentLinkedQueue[Promise[ToReturn]]()

  private val CRLF = "\r\n".getBytes
  private val DOLLAR = "$".getBytes
  private val ASTERISK_BYTE = "*".getBytes

  override def execute[T](command: String, args: Seq[String]): Future[T] = {
    executeBinary(command, args.map(_.getBytes))
  }

  val t = new Timer()
  t.scheduleAtFixedRate(new TimerTask {
    override def run(): Unit = {
      os.flush()
    }
  }, 5, 5)

  override def executeBinary[T](command: String, args: Seq[Array[Byte]]): Future[T] = {
    val promise = Promise[ToReturn]()
    try {
      val buffer = os

      buffer.write(ASTERISK_BYTE)
      buffer.write((1 + args.length).toString.getBytes)
      buffer.write(CRLF)
      writeBulkString(buffer, command.getBytes)
      args.foreach(arg => {
        writeBulkString(buffer, arg)
      })
    } catch {
      case ex: IOException =>
        promise.failure(ex)
    }
    requests.add(promise)

    promise.future.asInstanceOf[Future[T]]
  }

  private def writeBulkString(out: OutputStream, string: Array[Byte]): Unit = {
    out.write(DOLLAR)
    out.write(string.length.toString.getBytes)
    out.write(CRLF)
    out.write(string)
    out.write(CRLF)
  }

  private def onSuccess(data: AnyRef): Unit = {
    try {
      var poll = requests.poll()
      while (poll == null) {
        poll = requests.poll()
      }
      data match {
        case ex: RuntimeException =>
          poll.failure(ex)
        case data: Any =>
          poll.success(data)
      }
    } catch {
      case t: Throwable =>
        t.printStackTrace()
    }
    ()
  }

  private val x = Executors.newSingleThreadExecutor()
  x.submit(new ReaderThread)

  class ReaderThread extends Runnable {

    private val matcher = new EffectiveRedisParser
    private val buffer = new Array[Byte](1024)
    private val buf = Unpooled.wrappedBuffer(buffer)

    override def run(): Unit = {
      while (!Thread.interrupted()) {
        val readBytes = is.read(buffer)

        val toProcess = buf.slice(0, readBytes)

        parse(toProcess)
      }
    }

    private def parse(toProcess: ByteBuf): Unit = {
      while (true) {
        matcher.parse(toProcess) match {
          case ResponseNotReady =>
            return
          case data: AnyRef =>
            matcher.reset()
            onSuccess(data)
        }
      }
    }
  }

}
