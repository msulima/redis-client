package pl.msulima.redis.benchmark.repository

import java.util.concurrent.ConcurrentLinkedQueue

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.ByteToMessageDecoder
import io.netty.handler.codec.string.StringEncoder

import scala.concurrent.{Await, Future, Promise}

class RedisDecoder extends ByteToMessageDecoder {

  var matcher = RedisParser.matcher

  @throws(classOf[Exception])
  override protected def decode(ctx: ChannelHandlerContext, in: ByteBuf, out: java.util.List[AnyRef]): Unit = {
    matcher(in) match {
      case Left(v) =>
        matcher = RedisParser.matcher
        out.add(v)
      case Right(nextF) =>
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

  def execute[T](queries: Seq[String]): Future[T]
}

object NettyRedisClient extends App {

  import scala.concurrent.duration._

  val client = new NettyRedisClient("localhost", 6379)
  val p = client.execute(Seq("MGET", "foo", "bar"))
  println(Await.result(p, 10.seconds))
}

class NettyRedisClient(host: String, port: Int) extends RedisClient {

  private val group: EventLoopGroup = new NioEventLoopGroup
  private val requests = new ConcurrentLinkedQueue[Promise[ToReturn]]()
  private val encoder = new StringEncoder

  val ch: Channel = {
    val b: Bootstrap = new Bootstrap
    b.group(group).channel(classOf[NioSocketChannel])
    b.handler(new ChannelInitializer[SocketChannel]() {
      override def initChannel(ch: SocketChannel): Unit = {
        ch.pipeline().addLast(new RedisDecoder(), new TelnetClientHandler(onSuccess)).addLast(encoder)
      }
    })
    b.connect(host, port).sync.channel
  }

  private def onSuccess(data: AnyRef): Unit = {
    requests.poll().success(data)
    ()
  }

  private val CRLF = "\r\n"

  // non thread-safe
  override def execute[T](queries: Seq[String]): Future[T] = {
    val sb = new StringBuilder()
    sb.append("*")
    sb.append(queries.length.toString)
    sb.append(CRLF)
    queries.foreach(query => {
      sb.append("$")
      sb.append(query.length.toString)
      sb.append(CRLF)
      sb.append(query)
      sb.append(CRLF)
    })
    sb.append(CRLF)

    println(sb.toString())
    val x = ch.writeAndFlush(sb.toString())
    x.sync()

    val promise = Promise[ToReturn]()
    requests.add(promise)
    promise.future.asInstanceOf[Future[T]]
  }
}
