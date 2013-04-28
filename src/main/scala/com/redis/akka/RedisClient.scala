package com.redis.akka

import java.net.InetSocketAddress
import scala.concurrent.{Promise, ExecutionContext, Await}
import scala.collection.immutable.Queue
import scala.concurrent.duration._
import scala.util.control.Breaks._
import akka.pattern.{ask, pipe}
import akka.io.{Tcp, IO}
import Tcp._
import akka.util.{ByteString, Timeout}
import akka.actor._
import ExecutionContext.Implicits.global
import scala.annotation.tailrec
import scala.language.existentials

// example adopted from : http://rajivkurian.github.io/blog/2012/12/25/a-simple-redis-client-using-spray-io-actors-futures-and-promises/

import serialization._
import Parse.{Implicits => Parsers}

object RedisReplies {

  /**
   * Redis will reply to commands with different kinds of replies. It is always possible to detect the kind of reply 
   * from the first byte sent by the server:
   * <li> In a Status Reply the first byte of the reply is "+"</li>
   * <li> In an Error Reply the first byte of the reply is "-"</li>
   * <li> In an Integer Reply the first byte of the reply is ":"</li>
   * <li> In a Bulk Reply the first byte of the reply is "$"</li>
   * <li> In a Multi Bulk Reply the first byte of the reply s "*"</li>
   */

  /**
   * Response codes from the Redis server
   */
  val ERR    = '-'
  val OK     = "OK".getBytes("UTF-8")
  val QUEUED = "QUEUED".getBytes("UTF-8")
  val SINGLE = '+'
  val BULK   = '$'
  val MULTI  = '*'
  val INT    = ':'

  val LS     = "\r\n".getBytes("UTF-8")

  type Reply[T] = PartialFunction[(Char, Array[Byte], RedisReply), T]
  type SingleReply = Reply[Option[Array[Byte]]]
  type MultiReply = Reply[Option[List[Option[Array[Byte]]]]]
  val crlf = List(13, 10)

  def split(bytes: Array[Byte]): List[Array[Byte]] = {
    @tailrec
    def split_a(bytes: Array[Byte], acc: List[Array[Byte]], inter: Array[Byte]): List[Array[Byte]] = bytes match {
      case Array(13, 10, rest@_*) => split_a(rest.toArray, acc ::: List(inter), Array.empty[Byte])
      case Array(x, rest@_*) if x != 13 && x != 10 => split_a(rest.toArray, acc, inter :+ x)
      case Array() => acc
    }
    split_a(bytes, List.empty[Array[Byte]], Array.empty[Byte])
  }

  def protocol_split(bytes: Array[Byte]): List[Array[Byte]] = {
    def protocol_split_a(bytes: Array[Byte], acc: List[Array[Byte]]): List[Array[Byte]] = bytes match {
      case Array(43, x, y, 13, 10, rest@_*) => {
        protocol_split_a(rest.toArray, acc ::: List(Array[Byte]('+'.toByte, x, y, 13, 10)))
      }
      case o@Array(58, rest@_*) => {
        val (x, y) = rest.splitAt(rest.indexOfSlice(List(13, 10)) + 2)
        protocol_split_a(y.toArray, acc ::: List(o.splitAt(1)._1 ++ x))
      }
      case o@Array(36, l, 13, 10, rest@_*) => {
        val (x, y) = rest.splitAt(new String(Array[Byte](l), "UTF-8").toInt + 2)
        protocol_split_a(y.toArray, acc ::: List(o.splitAt(4)._1 ++ x))
      }
      case Array(42, rest@_*) => { 
        val(l, a) = rest.toArray.splitAt(rest.toArray.indexOfSlice(List(13, 10)) + 2)
        val r = protocol_split(a)
        val no = new String(l.dropRight(2), "UTF-8").toInt
        val (tojoin, others) = r.splitAt(no)
        acc ::: List(Array[Byte](42) ++ l ++ tojoin.tail.foldLeft(tojoin.head)(_ ++ _)) ::: others
        // acc ::: List(Array[Byte](42) ++ l ++ tojoin.tail.foldLeft(tojoin.head)(_ ++ Array[Byte]('\r', '\n') ++ _)) ::: others
      }
      case Array() => acc
    }
    protocol_split_a(bytes, List.empty[Array[Byte]])
  }

  case class RedisReply(s: Array[Byte]) {
    val iter = split(s).iterator
    def get: Option[Array[Byte]] = {
      if (iter.hasNext) Some(iter.next)
      else None
    }

    def receive[T](pf: Reply[T]) = get match {
      case Some(line) =>
        (pf orElse errReply) apply ((line(0).toChar,line.slice(1,line.length), this))
      case None => sys.error("Error in receive")
    }

    def asString: Option[String] = receive(singleLineReply) map Parsers.parseString

    def asBulk[T](implicit parse: Parse[T]): Option[T] =  receive(bulkReply) map parse
  
    def asBulkWithTime[T](implicit parse: Parse[T]): Option[T] = receive(bulkReply orElse multiBulkReply) match {
      case x: Some[Array[Byte]] => x.map(parse(_))
      case _ => None
    }

    def asLong: Option[Long] =  receive(longReply orElse queuedReplyLong)

    def asBoolean: Option[Boolean] = receive(longReply orElse singleLineReply) match {
      case Some(n: Int) => Some(n > 0)
      case Some(s: Array[Byte]) => Parsers.parseString(s) match {
        case "OK" => Some(true)
        case "QUEUED" => Some(true)
        case _ => Some(false)
      }
      case _ => None
    }

    def asList[T](implicit parse: Parse[T]): Option[List[Option[T]]] = receive(multiBulkReply).map(_.map(_.map(parse)))

    def asListPairs[A,B](implicit parseA: Parse[A], parseB: Parse[B]): Option[List[Option[(A,B)]]] =
      receive(multiBulkReply).map(_.grouped(2).flatMap{
        case List(Some(a), Some(b)) => Iterator.single(Some((parseA(a), parseB(b))))
        case _ => Iterator.single(None)
      }.toList)

    def asQueuedList: Option[List[Option[String]]] = receive(queuedReplyList).map(_.map(_.map(Parsers.parseString)))

    // def asExec(handlers: Seq[() => Any]): Option[List[Any]] = receive(execReply(handlers))

    def asSet[T: Parse]: Option[Set[Option[T]]] = asList map (_.toSet)

    def asAny = receive(longReply orElse singleLineReply orElse bulkReply orElse multiBulkReply)
  }

  val longReply: Reply[Option[Long]] = {
    case (INT, s, _) => Some(Parsers.parseLong(s))
    case (BULK, s, _) if Parsers.parseInt(s) == -1 => None
  }

  val singleLineReply: SingleReply = {
    case (SINGLE, s, _) => Some(s)
    case (INT, s, _) => Some(s)
  }

  val bulkReply: SingleReply = {
    case (BULK, s, r) => 
      val next = r.get
      Parsers.parseInt(s) match {
        case -1 => None
        case x if x == next.get.size => next
        case _ => None
      }
  }

  val multiBulkReply: MultiReply = {
    case (MULTI, str, r) =>
      Parsers.parseInt(str) match {
        case -1 => None
        case n => Some(List.fill(n)(r.receive(bulkReply orElse singleLineReply)))
      }
  }

  val errReply: Reply[Nothing] = {
    case (ERR, s, _) => throw new Exception(Parsers.parseString(s))
    case x => throw new Exception("Protocol error: Got " + x + " as initial reply byte")
  }

  def execReply(handlers: Seq[() => Any]): PartialFunction[(Char, Array[Byte]), Option[List[Any]]] = {
    case (MULTI, str) =>
      Parsers.parseInt(str) match {
        case -1 => None
        case n if n == handlers.size => 
          Some(handlers.map(_.apply).toList)
        case n => throw new Exception("Protocol error: Expected "+handlers.size+" results, but got "+n)
      }
  }

  def queuedReplyInt: Reply[Option[Int]] = {
    case (SINGLE, QUEUED, _) => Some(Int.MaxValue)
  }
  
  def queuedReplyLong: Reply[Option[Long]] = {
    case (SINGLE, QUEUED, _) => Some(Long.MaxValue)
    }

  def queuedReplyList: MultiReply = {
    case (SINGLE, QUEUED, _) => Some(List(Some(QUEUED)))
  }
}


object RedisCommands {
  import RedisReplies._

  def multiBulk(args: Seq[Array[Byte]]): Array[Byte] = {
    val b = new scala.collection.mutable.ArrayBuilder.ofByte
    b ++= "*%d".format(args.size).getBytes
    b ++= LS
    args foreach { arg =>
      b ++= "$%d".format(arg.size).getBytes
      b ++= LS
      b ++= arg
      b ++= LS
    }
    b.result
  }

  sealed trait RedisCommand[R] {
    def line: Array[Byte]
    def promise: Promise[_]
    def reply(s: Array[Byte]): Option[R]
  }

  case class Get[A](key: Any)(implicit format: Format, parse: Parse[A]) extends RedisCommand[A] {
    val line = multiBulk("GET".getBytes("UTF-8") +: Seq(format.apply(key)))
    val promise = Promise[Option[A]]
    def reply(s: Array[Byte]) = {
      val r = RedisReply(s).asBulk[A]
      promise.success(r)
      r
    }
  }
  case class Set(key: Any, value: Any)(implicit format: Format) extends RedisCommand[Boolean] {
    val line = multiBulk("SET".getBytes("UTF-8") +: (Seq(key, value) map format.apply))
    val promise = Promise[Option[Boolean]]
    def reply(s: Array[Byte]) = {
      val r = RedisReply(s).asBoolean
      promise.success(r)
      r
    }
  }
  case class LPush(key: Any, value: Any)(implicit format: Format) extends RedisCommand[Long] {
    val line = multiBulk("LPUSH".getBytes("UTF-8") +: (Seq(key, value) map format.apply))
    val promise = Promise[Option[Long]]
    def reply(s: Array[Byte]) = {
      val r = RedisReply(s).asLong
      promise.success(r)
      r
    }
  }
  case class RPush(key: Any, value: Any)(implicit format: Format) extends RedisCommand[Long] {
    val line = multiBulk("RPUSH".getBytes("UTF-8") +: (Seq(key, value) map format.apply))
    val promise = Promise[Option[Long]]
    def reply(s: Array[Byte]) = {
      val r = RedisReply(s).asLong
      promise.success(r)
      r
    }
  }
  case class LRange[A](key: Any, start: Int, stop: Int)(implicit format: Format, parse: Parse[A]) 
    extends RedisCommand[List[Option[A]]] {
    val line = multiBulk("LRANGE".getBytes("UTF-8") +: (Seq(key, start, stop) map format.apply))
    val promise = Promise[Option[List[Option[A]]]]
    def reply(s: Array[Byte]) = {
      val r = RedisReply(s).asList
      promise.success(r)
      r
    }
  }
}

class RedisClient(remote: InetSocketAddress) extends Actor {
  import Tcp._
  import context.system
  import RedisCommands._
  import RedisReplies._

  class KeyNotFoundException extends Exception
  val promiseQueue = new scala.collection.mutable.Queue[RedisCommand[_]]
  IO(Tcp) ! Connect(remote)

  def receive = {
    case CommandFailed(_: Connect) =>
      println("failed")
      context stop self

    case c@ Connected(remote, local) =>
      println(c)
      val connection = sender
      connection ! Register(self)

      context become {
        case command: RedisCommand[_] => 
          println("got command: " + command)
          sendRedisCommand(connection, command)

        case Received(data) => // redis replies 
          println("got data: " + data.decodeString("UTF-8"))
          val responseArray = data.toArray[Byte]
          val replies = protocol_split(responseArray)
          replies.map {r =>
            promiseQueue.dequeue reply r
          }

        case CommandFailed(w: Write) => println("fatal")
        case "close" => connection ! Close
        case _: ConnectionClosed => context stop self
      }
  }
  def sendRedisCommand(conn: ActorRef, command: RedisCommand[_])(implicit ec: ExecutionContext) = {
    conn ! Write(ByteString(command.line))
    promiseQueue += command
    val f = command.promise.future
    pipe(f) to sender
  }
}

object RedisClient {

  implicit val system = ActorSystem("redis-client")

  val endpoint = new InetSocketAddress("localhost", 6379)
  val client = system.actorOf(Props(new RedisClient(endpoint)), name = "redis-client")
  implicit val timeout = Timeout(5 seconds)

  def run() {

    val numKeys = 3
    val (keys, values) = (1 to numKeys map { num => ("hello" + num, "world" + num) }).unzip
    import RedisCommands._
    Thread.sleep(2000)

    // set a bunch of stuff
    val writeResults = keys zip values map { case (key, value) =>
      (key, value, (client ? Set(key, value)).mapTo[Option[Boolean]])
    }

    writeResults foreach { case (key, value, result) =>
      result onSuccess {
        case Some(someBoolean) if someBoolean == true => println("Set " + key + " to value " + value)
        case _ => println("Failed to set key " + key + " to value " + value)
      }
      result onFailure {
        case t: Exception => t.printStackTrace; println("Failure: Failed to set key " + key + " to value " + value + " : " + t)
      }
    }

    // get the ones set earlier
    val readResults = keys map { key => (key, client.ask(Get[String](key)).mapTo[Option[String]]) }
    readResults zip values foreach { case ((key, result), expectedValue) =>
      result.onSuccess {
        case resultString =>
          println("Got a result for " + key + ": "+ resultString)
          assert(resultString == Some(expectedValue))
      }
      result.onFailure {
        case t => println("Got some exception " + t)
      }
    }

    // lpush a bunch of stuff
    val forpush = List.fill(10)("listk") zip (1 to 10).map(_.toString)
    val writeListResults = forpush map { case (key, value) =>
      (key, value, (client ? LPush(key, value)).mapTo[Option[Long]])
    }

    writeListResults foreach { case (key, value, result) =>
      result onSuccess {
        case Some(someLong) if someLong > 0 => println("Set " + key + " to value " + value + " and returned " + someLong)
        case _ => println("Failed to set key " + key + " to value " + value)
      }
      result onFailure {
        case t => println("Failed to set key " + key + " to value " + value + " : " + t)
      }
    }
    writeListResults.map(e => Await.result(e._3, 3 seconds))

    // do an lrange to check if they were inserted correctly & in proper order
    val readListResult = client.ask(LRange[String]("listk", 0, -1)).mapTo[Option[List[String]]]
    readListResult.onSuccess {
      case result =>
        println("Range = " + result)
        assert(result == Some(List(Some("10"), Some("9"), Some("8"), Some("7"), Some("6"), Some("5"), Some("4"), Some("3"), Some("2"), Some("1"))))
    }
    readListResult.onFailure {
      case t => println("Got some exception " + t)
    }

    // rpush a bunch of stuff
    val forrpush = List.fill(10)("listr") zip (1 to 10).map(_.toString)
    val writeListRes = forrpush map { case (key, value) =>
      (key, value, (client ? RPush(key, value)).mapTo[Option[Long]])
    }
    writeListRes.map(e => Await.result(e._3, 3 seconds))

    // do an lrange to check if they were inserted correctly & in proper order
    val readListRes = client.ask(LRange[String]("listr", 0, -1)).mapTo[Option[List[String]]]
    readListRes.onSuccess {
      case result =>
        println("Range = " + result)
        assert(result.map(_.reverse) == Some(List(Some("10"), Some("9"), Some("8"), Some("7"), Some("6"), Some("5"), Some("4"), Some("3"), Some("2"), Some("1"))))
    }
    readListRes.onFailure {
      case t => println("Got some exception " + t)
    }
  }
}
