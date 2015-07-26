package pl.msulima.redis.benchmark.repository


import java.util


object RedisSingleInt {

  def apply(i: Int) = s":$i"

  def unapply(s: String): Option[Int] = {
    if (s.charAt(0) == ':') {
      Some(s.substring(1).toInt)
    } else {
      None
    }
  }
}

object RedisNil {

  def apply(s: Unit) = "$-1"

  def unapply(s: String): Option[Unit] = {
    if (s == "$-1") {
      Some(())
    } else {
      None
    }
  }
}

object RedisStringLength {

  def apply(s: Int) = s"$$$s"

  def unapply(s: String): Option[Int] = {
    if (s.charAt(0) == '$') {
      Some(s.substring(1).toInt)
    } else {
      None
    }
  }
}

object RedisArrayLength {

  def apply(s: Int) = s"*$s"

  def unapply(s: String): Option[Int] = {
    if (s.charAt(0) == '*') {
      Some(s.substring(1).toInt)
    } else {
      None
    }
  }
}


object RedisArray {

  def apply(a: Seq[Any]): util.Queue[String] = {
    val q = new util.LinkedList[String]
    // todo q.addAll(a.asJava)
    q
  }

  def unapply(q: util.Queue[String]): Option[Seq[Any]] = {
    q.peek() match {
      case RedisArrayLength(length) =>
        q.remove()
        val builder = Seq.newBuilder[Any]
        for (y <- 1 to length) {
          builder += RedisDeserializer(q)
        }
        Some(builder.result())
      case _ =>
        None
    }
  }
}

object RedisInt {

  def apply(a: Int): util.Queue[String] = {
    val q = new util.LinkedList[String]
    q.add(a.toString)
    q
  }

  def unapply(q: util.Queue[String]): Option[Int] = {
    q.peek() match {
      case RedisSingleInt(value) =>
        q.remove()
        Some(value)
      case _ =>
        None
    }
  }
}

sealed trait RedisCommand {

  def deserialize(queue: util.Queue[String]): Any
}

object RedisString {

  def apply(a: String): util.Queue[String] = {
    val q = new util.LinkedList[String]
    q.add(a)
    q
  }

  def unapply(q: util.Queue[String]): Option[String] = {
    q.peek() match {
      case RedisStringLength(length) =>
        q.remove()
        Some(q.poll())
      case _ =>
        None
    }
  }

}

class RedisString(length: Int) {

  def dafuq(queue: String): Either[Any, (String) => Either[Any, (String) => Any]] = {
    Left(Some(queue))
  }
}

class RedisArray private(length: Int, acc: Seq[Any]) {

  def this(length: Int) = this(length, List[Any]())

  def dafuq(queue: String): Either[Any, (String) => Either[Any, (String) => Any]] = {
    queue match {
      case RedisNil(value) =>
        step(acc :+ null)
      case RedisSingleInt(value) =>
        step(acc :+ value)
      case RedisStringLength(s) =>
        Right(stringDafuq _)
    }
  }

  private def stringDafuq(queue: String): Either[Any, (String) => Either[Any, (String) => Any]] = {
    step(acc :+ queue)
  }

  private def step(acc: Seq[Any]) = {
    if (acc.size == length) {
      Left(acc)
    } else {
      Right(new RedisArray(length, acc).dafuq _)
    }
  }
}

object RedisDeserializer {

  type Matcher = (String) => Either[Any, (String) => Any]

  def dafuq(queue: String): Either[Any, (String) => Any] = {
    queue match {
      case RedisArrayLength(length) =>
        println("arr")
        Right(new RedisArray(length).dafuq _)
      case RedisNil(value) =>
        Left(null)
      case RedisSingleInt(value) =>
        Left(value)
      case RedisStringLength(s) =>
        Right(new RedisString(s).dafuq _)
    }
  }

  def apply(queue: util.Queue[String]): Any = {
    queue match {
      case RedisArray(arr) =>
        arr
      case RedisInt(value) =>
        value
      case RedisString(s) =>
        s
    }
  }
}
