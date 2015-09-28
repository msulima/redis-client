package pl.msulima.redis.benchmark.repository

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Timer, TimerTask}

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

  private val matcher = new EffectiveRedisParser

  @throws(classOf[Exception])
  override protected def decode(ctx: ChannelHandlerContext, in: ByteBuf, out: java.util.List[AnyRef]): Unit = {
    val x = new Array[Byte](in.readableBytes())
    in.getBytes(in.readerIndex(), x)
    matcher.parse(in) match {
      case ResponseNotReady =>
      case v: AnyRef =>
        matcher.reset()
        out.add(v)
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

class RoutingRedisClient extends RedisClient {

  val current = new AtomicInteger()
  private val size = 5
  val clients = (1 to size).map(_ => new SyncClient())

  def execute[T](command: String, args: Seq[String]): Future[T] = {
    clients(current.incrementAndGet() % size).execute(command, args)
  }

  def executeBinary[T](command: String, args: Seq[Array[Byte]]): Future[T] = {
    clients(current.incrementAndGet() % size).executeBinary(command, args)
  }
}

object NettyRedisClient extends App {

  import scala.concurrent.duration._

  val client = new NettyRedisClient("localhost", 6379)
  val p = client.execute("MGET", Seq("foo", "bar"))
  println(Await.result(p, 10.seconds).asInstanceOf[Array[AnyRef]].mkString)
}

class NettyRedisClient(host: String, port: Int) extends RedisClient {

  private val group: EventLoopGroup = new NioEventLoopGroup(16)
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

  private val CRLF = "\r\n".getBytes
  private val DOLLAR = "$".getBytes
  private val STAR = "*"

  override def execute[T](command: String, args: Seq[String]): Future[T] = {
    executeBinary(command, args.map(_.getBytes))
  }

  val t = new Timer()
  t.scheduleAtFixedRate(new TimerTask {
    override def run(): Unit = {
      ch.flush()
    }
  }, 5, 5)

  override def executeBinary[T](command: String, args: Seq[Array[Byte]]): Future[T] = {
    val buffer = ch

    buffer.write(STAR.getBytes)
    buffer.write((1 + args.length).toString.getBytes)
    buffer.write(CRLF)
    writeBulkString(buffer, command.getBytes)
    args.foreach(arg => {
      writeBulkString(buffer, arg)
    })

    val x = ch.write(Array[Byte]())

    val promise = Promise[ToReturn]()
    requests.add(promise)

    x.addListener(new ChannelFutureListener {
      override def operationComplete(future: ChannelFuture): Unit = {
        if (!future.isSuccess) {
          promise.failure(new RuntimeException("Netty fail", future.cause()))
        }
      }
    })

    promise.future.asInstanceOf[Future[T]]
  }

  private def writeBulkString(out: Channel, string: Array[Byte]): Unit = {
    out.write(DOLLAR)
    out.write(string.length.toString.getBytes)
    out.write(CRLF)
    out.write(string)
    out.write(CRLF)
  }
}
