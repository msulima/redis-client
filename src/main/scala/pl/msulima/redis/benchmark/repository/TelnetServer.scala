package pl.msulima.redis.benchmark.repository
package pl.msulima.redis.benchmark.repository

import java.net.InetAddress
import java.util.Date

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelFuture, ChannelFutureListener, ChannelHandlerContext, ChannelInitializer, ChannelPipeline, EventLoopGroup, SimpleChannelInboundHandler}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.{DelimiterBasedFrameDecoder, Delimiters}
import io.netty.handler.codec.string.{StringDecoder, StringEncoder}
import io.netty.handler.logging.{LogLevel, LoggingHandler}

/**
 * Creates a newly configured {@link ChannelPipeline} for a new channel.
 */
object TelnetServerInitializer {
  private val DECODER: StringDecoder = new StringDecoder
  private val ENCODER: StringEncoder = new StringEncoder
  private val SERVER_HANDLER: TelnetServerHandler = new TelnetServerHandler
}

class TelnetServerInitializer extends ChannelInitializer[SocketChannel] {

  @throws(classOf[Exception])
  def initChannel(ch: SocketChannel) {
    val pipeline: ChannelPipeline = ch.pipeline
    pipeline.addLast(new DelimiterBasedFrameDecoder(8192, Delimiters.lineDelimiter(): _*))
    pipeline.addLast(TelnetServerInitializer.DECODER)
    pipeline.addLast(TelnetServerInitializer.ENCODER)
    pipeline.addLast(TelnetServerInitializer.SERVER_HANDLER)
  }
}

/**
 * Handles a server-side channel.
 */
@Sharable class TelnetServerHandler extends SimpleChannelInboundHandler[String] {
  @throws(classOf[Exception])
  override def channelActive(ctx: ChannelHandlerContext) {
    ctx.write("Welcome to " + InetAddress.getLocalHost.getHostName + "!\r\n")
    ctx.write("It is " + new Date + " now.\r\n")
    ctx.flush
  }

  @throws(classOf[Exception])
  def channelRead0(ctx: ChannelHandlerContext, request: String) {
    var response: String = null
    var close: Boolean = false
    if (request.isEmpty) {
      response = "Please type something.\r\n"
    }
    else if ("bye" == request.toLowerCase) {
      response = "Have a good day!\r\n"
      close = true
    }
    else {
      response = "Did you say '" + request + "'?\r\n"
    }
    val future: ChannelFuture = ctx.write(response)
    if (close) {
      future.addListener(ChannelFutureListener.CLOSE)
    }
  }

  override def channelReadComplete(ctx: ChannelHandlerContext) {
    ctx.flush
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    cause.printStackTrace
    ctx.close
  }
}

/**
 * Simplistic telnet server.
 */
object TelnetServer {
  private[repository] val PORT: Int = System.getProperty("port", "8024").toInt

  @throws(classOf[Exception])
  def main(args: Array[String]) {
    val bossGroup: EventLoopGroup = new NioEventLoopGroup(1)
    val workerGroup: EventLoopGroup = new NioEventLoopGroup
    try {
      val b: ServerBootstrap = new ServerBootstrap
      b.group(bossGroup, workerGroup).channel(classOf[NioServerSocketChannel])
        .handler(new LoggingHandler(LogLevel.INFO)).childHandler(new TelnetServerInitializer())
      b.bind(PORT).sync.channel.closeFuture.sync
    } finally {
      bossGroup.shutdownGracefully
      workerGroup.shutdownGracefully
    }
  }
}


