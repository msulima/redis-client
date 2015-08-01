package pl.msulima.redis.benchmark.repository

import java.util.concurrent.ConcurrentLinkedQueue

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.ByteToMessageDecoder
import io.netty.handler.codec.bytes.ByteArrayEncoder

import scala.concurrent.{Await, Future, Promise}

class RedisDecoder extends ByteToMessageDecoder {

  var matcher = RedisParser.matcher

  @throws(classOf[Exception])
  override protected def decode(ctx: ChannelHandlerContext, in: ByteBuf, out: java.util.List[AnyRef]): Unit = {
    val x = new Array[Byte](in.readableBytes())
    in.getBytes(in.readerIndex(), x)
    println(Bytes.debug(x), this, Thread.currentThread().getId)
    matcher(in) match {
      case Left(v) =>
        matcher = RedisParser.matcher
        println(Bytes.debugResult(v))
        out.add(v)
      case Right(nextF) =>
        println("nextF...")
        matcher = nextF.asInstanceOf[RedisParser.Matcher]
    }
  }
}

class TelnetClientHandler(onSuccess: (AnyRef) => Unit) extends ChannelInboundHandlerAdapter {

  override def channelRead(ctx: ChannelHandlerContext, msg: AnyRef): Unit = {
    onSuccess(msg)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    cause.printStackTrace()
    ctx.close
  }
}

trait RedisClient {

  type ToReturn = AnyRef

  def execute[T](command: String, args: Seq[String]): Future[T]

  def executeBinary[T](command: String, args: Seq[Array[Byte]]): Future[T]
}

object NettyRedisClient extends App {

  import scala.concurrent.duration._

  val client = new NettyRedisClient("localhost", 6379)
  val p = client.execute("MGET", Seq("foo", "bar"))
  println(Await.result(p, 10.seconds).asInstanceOf[Array[AnyRef]].mkString)
}

class NettyRedisClient(host: String, port: Int) extends RedisClient {

  private val group: EventLoopGroup = new NioEventLoopGroup
  private val requests = new ConcurrentLinkedQueue[Promise[ToReturn]]()

  val ch: Channel = {
    val b: Bootstrap = new Bootstrap
    b.group(group).channel(classOf[NioSocketChannel])
    b.handler(new ChannelInitializer[SocketChannel]() {
      override def initChannel(ch: SocketChannel): Unit = {
        ch.pipeline().addLast(new RedisDecoder(), new TelnetClientHandler(onSuccess)).addLast(new ByteArrayEncoder)
      }
    })
    b.connect(host, port).sync.channel
  }

  private def onSuccess(data: AnyRef): Unit = {
    try {
      var poll = requests.poll()
      while (poll == null) {
        poll = requests.poll()
      }
      poll.success(data)
    } catch {
      case t: Throwable =>
        t.printStackTrace()
    }
    ()
  }

  private val CRLF = "\r\n".getBytes

  override def execute[T](command: String, args: Seq[String]): Future[T] = {
    executeBinary(command, args.map(_.getBytes))
  }

  override def executeBinary[T](command: String, args: Seq[Array[Byte]]): Future[T] = {
    ch.write("*".getBytes)
    ch.write((1 + args.length).toString.getBytes)
    ch.write(CRLF)
    writeBulkString(ch, command.getBytes)
    args.foreach(arg => {
      writeBulkString(ch, arg)
    })

    val x = ch.writeAndFlush(Array[Byte]())

    x.sync()

    val promise = Promise[ToReturn]()
    requests.add(promise)

    promise.future.asInstanceOf[Future[T]]
  }

  private def writeBulkString(out: Channel, string: Array[Byte]): Unit = {
    out.write("$".getBytes)
    out.write(string.length.toString.getBytes)
    out.write(CRLF)
    out.write(string)
    out.write(CRLF)
  }
}
