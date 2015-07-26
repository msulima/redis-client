package pl.msulima.redis.benchmark.repository

import java.util.concurrent.ConcurrentLinkedQueue

import io.netty.bootstrap.Bootstrap
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.channel.{Channel, ChannelFuture, ChannelHandlerContext, ChannelInitializer, ChannelPipeline, EventLoopGroup, SimpleChannelInboundHandler}
import io.netty.handler.codec.string.{StringDecoder, StringEncoder}
import io.netty.handler.codec.{DelimiterBasedFrameDecoder, Delimiters}

import scala.concurrent.{Await, Future, Promise}

object TelnetClientInitializer {
  private val DECODER: StringDecoder = new StringDecoder
  private val ENCODER: StringEncoder = new StringEncoder
}

class TelnetClientInitializer(handler: TelnetClientHandler) extends ChannelInitializer[SocketChannel] {

  def initChannel(ch: SocketChannel) {
    val pipeline: ChannelPipeline = ch.pipeline
    pipeline.addLast(new DelimiterBasedFrameDecoder(8192, Delimiters.lineDelimiter(): _*))
    pipeline.addLast(TelnetClientInitializer.DECODER)
    pipeline.addLast(TelnetClientInitializer.ENCODER)
    pipeline.addLast(handler)
  }
}

class TelnetClientHandler(onSuccess: (Any) => Unit) extends SimpleChannelInboundHandler[String] {

  var matcher = RedisDeserializer.dafuq _

  @throws(classOf[Exception])
  protected def channelRead0(ctx: ChannelHandlerContext, msg: String) {
    System.err.println(s"hop $msg")
    matcher(msg) match {
      case Left(v) =>
        println(s"found $v")
        matcher = RedisDeserializer.dafuq
        onSuccess(v)
      case Right(nextF) =>
        println(s"processing...")
        matcher = nextF.asInstanceOf[RedisDeserializer.Matcher]
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    cause.printStackTrace()
    ctx.close
  }
}

trait RedisClient {

  type ToReturn = Any

  def execute(queries: Seq[String]): Future[ToReturn]
}

object TelnetClient extends App {

  import scala.concurrent.duration._

  val client = new TelnetClient("localhost", 6379)
  val p = client.execute(Seq("MGET", "foo", "bar"))
  println(Await.result(p, 10.seconds))
}

class TelnetClient(host: String, port: Int) extends RedisClient {

  private val group: EventLoopGroup = new NioEventLoopGroup
  private val requests = new ConcurrentLinkedQueue[Promise[ToReturn]]()

  val ch: Channel = {
    val b: Bootstrap = new Bootstrap
    val handler = new TelnetClientHandler(onSuccess)
    b.group(group).channel(classOf[NioSocketChannel]).handler(new TelnetClientInitializer(handler))
    b.connect(host, port).sync.channel
  }

  var lastWriteFuture: ChannelFuture = null

  private def onSuccess(data: Any): Unit = {
    requests.poll().success(data)
    ()
  }

  private val CRLF = "\r\n"

  // non thread-safe
  override def execute(queries: Seq[String]): Future[ToReturn] = {
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
    lastWriteFuture = ch.writeAndFlush(sb.toString())

    if (lastWriteFuture != null) {
      lastWriteFuture.sync
    }

    val promise = Promise[ToReturn]()
    requests.add(promise)
    promise.future
  }
}
