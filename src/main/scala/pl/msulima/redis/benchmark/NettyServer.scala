package pl.msulima.redis.benchmark

import java.nio.charset.Charset
import java.util.concurrent.Executors

import akka.actor.ActorSystem
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.{ChannelFutureListener, ChannelHandlerContext, ChannelInboundHandlerAdapter, ChannelInitializer, ChannelOption}
import io.netty.handler.codec.http.HttpHeaders.Names._
import io.netty.handler.codec.http.HttpHeaders.Values
import io.netty.handler.codec.http.HttpResponseStatus._
import io.netty.handler.codec.http.HttpVersion._
import io.netty.handler.codec.http._
import pl.msulima.redis.benchmark.repository.Repository
import pl.msulima.redis.benchmark.serialization.AvroItemSerDe

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

object NettyServer extends App {

  private val Port = System.getProperty("port", "8080").toInt

  private val bossGroup = new NioEventLoopGroup(1)
  private val workerGroup = new NioEventLoopGroup()

  try {
    val b = new ServerBootstrap()
    b.option[Integer](ChannelOption.SO_BACKLOG, 1024)
    b.group(bossGroup, workerGroup)
      .channel(classOf[NioServerSocketChannel])
      //      .handler(new LoggingHandler(LogLevel.INFO))
      .childHandler(new HttpHelloWorldServerInitializer())

    val ch = b.bind(Port).sync.channel
    System.err.println("Open your web browser and navigate to http://127.0.0.1:" + Port + '/')
    ch.closeFuture.sync
  } finally {
    bossGroup.shutdownGracefully()
    workerGroup.shutdownGracefully()
  }
}

class HttpHelloWorldServerInitializer extends ChannelInitializer[SocketChannel] {
  def initChannel(ch: SocketChannel) {
    val p = ch.pipeline
    p.addLast(new HttpServerCodec)
    p.addLast(new HttpHelloWorldServerHandler())
  }
}

object HttpHelloWorldServerHandler {
  private implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(24))
}

class HttpHelloWorldServerHandler extends ChannelInboundHandlerAdapter {

  import HttpHelloWorldServerHandler._

  override def channelReadComplete(ctx: ChannelHandlerContext) {
    ctx.flush
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: AnyRef) {
    msg match {
      case req: HttpRequest =>
        if (HttpHeaders.is100ContinueExpected(req)) {
          ctx.write(new DefaultFullHttpResponse(HTTP_1_1, CONTINUE))
        }

        RequestHandler(req).foreach { response =>
          response.headers.set(CONTENT_TYPE, "text/plain")
          response.headers.set(CONTENT_LENGTH, response.content.readableBytes)

          val keepAlive = HttpHeaders.isKeepAlive(req)
          if (!keepAlive) {
            ctx.write(response).addListener(ChannelFutureListener.CLOSE)
          } else {
            response.headers.set(CONNECTION, Values.KEEP_ALIVE)
            ctx.write(response)
          }
          ctx.flush();
        }
      case _ =>
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    cause.printStackTrace()
    ctx.close
  }
}

object RequestHandler extends RepositoryRegistry {

  private val Concrete = "/concrete/(\\d+)".r
  private val Random = "/random/(\\d+)".r
  private val serializer = new AvroItemSerDe

  private val DefaultCharset = Charset.forName("UTF-8")

  private val Routes = Seq(
    testRoute("jedis/akka-pipelined", jedisAkkaPipelinedRepository),
    testRoute("jedis/akka-batch", jedisAkkaBatchRepository),
    testRoute("jedis/pipelined", jedisPipelinedRepository),
    testRoute("jedis/multi", jedisMultiGetRepository),
    testRoute("netty/simple", nettyRepository),
    testRoute("brando/multi", brandoMultiGetRepository)
  )

  override implicit lazy val system = ActorSystem()
  private implicit lazy val ec = system.dispatcher

  private def testRoute(name: String, sut: Repository): PartialFunction[HttpRequest, Future[ByteBuf]] = {
    case request if request.getUri.startsWith(s"/$name") && internalTestRoute(sut).isDefinedAt((request.getUri.stripPrefix(s"/$name"), request.getMethod)) =>
      internalTestRoute(sut)((request.getUri.stripPrefix(s"/$name"), request.getMethod))
  }

  private def internalTestRoute(sut: Repository): PartialFunction[(String, HttpMethod), Future[ByteBuf]] = {
    case (Concrete(id), HttpMethod.GET) =>
      sut.mget(Seq(id)).map(item => {
        Unpooled.copiedBuffer(item.headOption.map(i => serializer.toJSON(i)).getOrElse(""), DefaultCharset)
      })
    case (Random(id), HttpMethod.GET) =>
      val keys = KeysGenerator.get(id.toInt)

      val sequence = Future.sequence(keys.grouped(KeysGenerator.GroupSize).map(sut.mget(_))).map(_.flatten.toSeq)

      sequence.map(i => Unpooled.copiedBuffer(keys.zip(i.map(serializer.deserialize)).toString(), DefaultCharset))
    case (Random(id), HttpMethod.PUT) =>
      sut.mset(KeysGenerator.set(id.toInt).map(k => k._1 -> serializer.serialize(k._2))).map(x => {
        Unpooled.copiedBuffer(x.map(k => k._1 -> serializer.deserialize(k._2)).toString(), DefaultCharset)
      })
  }

  def apply(req: HttpRequest)(implicit ec: ExecutionContext): Future[FullHttpResponse] = {
    Routes.find(_.isDefinedAt(req)).map(_.apply(req).map(res => {
      new DefaultFullHttpResponse(HTTP_1_1, OK, res)
    }).recover({
      case NonFatal(ex) =>
        ex.printStackTrace()
        new DefaultFullHttpResponse(HTTP_1_1, INTERNAL_SERVER_ERROR, Unpooled.copiedBuffer("Error: " + ex.getMessage, DefaultCharset))
    })).getOrElse({
      Future(new DefaultFullHttpResponse(HTTP_1_1, NOT_FOUND, Unpooled.copiedBuffer("Not found " + req.getUri, DefaultCharset)))
    })
  }
}
