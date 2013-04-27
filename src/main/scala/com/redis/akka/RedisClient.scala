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
  }

    /**
    case class RedisReply(s: ByteString) {
      def get = s.head match {
        case INT => longReply apply ((s.head.toChar, s.tail.dropRight(2), ByteString.empty))
        case SINGLE => singleLineReply apply ((s.head.toChar, s.tail.dropRight(2), ByteString.empty))
        case BULK => {
          val (length, snd) = s.tail.splitAt(s.indexOfSlice(crlf))
          val body = snd.drop(1).take(length.head.toInt)
          bulkReply apply ((s.head.toChar, length.head, body))
        }
        case MULTI => {
          val (noOfReplies, snd) = s.tail.splitAt(s.indexOfSlice(crlf))
          val body = snd.drop(1).take(noOfReplies.head.toInt)
          multiBulkReply apply ((s.head.toChar, noOfReplies.head, body))
        }
      }
    }
    **/

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
}


object RedisCommands {
  sealed trait RedisCommand {
    def line: String
  }

  case class Get[A](key: String)(implicit format: Format, parse: Parse[A]) extends RedisCommand {
    val line = "*2\r\n" +
               "$3\r\n" +
               "GET\r\n" +
               "$" + key.length + "\r\n" +
               key + "\r\n"
  }
  case class Set(key: String, value: String)(implicit format: Format) extends RedisCommand {
    val line = "*3\r\n" +
               "$3\r\n" +
               "SET\r\n" +
               "$" + key.length + "\r\n" +
               key + "\r\n" +
               "$" + value.length + "\r\n" +
               value + "\r\n"
    lazy val wire = ByteString(line, "UTF-8")
  }
  case class LPush(key: String, value: String) extends RedisCommand {
    val line = "*3\r\n" +
               "$5\r\n" +
               "LPUSH\r\n" +
               "$" + key.length + "\r\n" +
               key + "\r\n" +
               "$" + value.length + "\r\n" +
               value + "\r\n"
  }
  case class RPush(key: String, value: String) extends RedisCommand {
    val line = "*3\r\n" +
               "$5\r\n" +
               "RPUSH\r\n" +
               "$" + key.length + "\r\n" +
               key + "\r\n" +
               "$" + value.length + "\r\n" +
               value + "\r\n"
  }
  case class LRange[A](key: String, start: Int, stop: Int) extends RedisCommand {
    val line = "*4\r\n" +
               "$6\r\n" +
               "LRANGE\r\n" +
               "$" + key.length + "\r\n" +
               key + "\r\n" +
               "$" + start.toString.length + "\r\n" +
               start + "\r\n" +
               "$" + stop.toString.length + "\r\n" +
               stop + "\r\n"
  }
}

class RedisClient(remote: InetSocketAddress) extends Actor {
  import Tcp._
  import context.system
  import RedisCommands._

  class KeyNotFoundException extends Exception
  val promiseQueue = new scala.collection.mutable.Queue[Promise[_]]
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
        case data: RedisCommand if data.line.startsWith("*3") => // rpush command 
          println("got rpush: " + data)
          sendRedisCommand(connection, ByteString(data.line, "UTF-8"), Promise[Int]) 

        case data: ByteString if data.decodeString("UTF-8").startsWith("*3") => // set command 
          println("got set or lpush: " + data.decodeString("UTF-8"))
          sendRedisCommand(connection, data, Promise[Boolean]) 

        case data: ByteString if data.decodeString("UTF-8").startsWith("*2") => // get command 
          println("got get: " + data.decodeString("UTF-8"))
          sendRedisCommand(connection, data, Promise[String]) 

        case data: ByteString if data.decodeString("UTF-8").startsWith("*4") => // lrange command 
          println("got lrange: " + data.decodeString("UTF-8"))
          sendRedisCommand(connection, data, Promise[List[String]]) 

        case Received(data) => // redis replies 
          val r = data.decodeString("UTF-8")
          val responseArray = r.split("\r\n")
          breakable {
            responseArray.zipWithIndex.foreach { case (response, index) =>
              if (response startsWith "+") { // set
                val setAnswer = response.substring(1, response.length)
                val nextPromise = promiseQueue.dequeue.asInstanceOf[Promise[Boolean]]
                nextPromise success (if (setAnswer == "OK") true else false)
              } else if (response startsWith "$") { // get
                val nextPromise = promiseQueue.dequeue.asInstanceOf[Promise[String]]
                if (response endsWith "-1") {
                  nextPromise failure (new KeyNotFoundException)
                } else {
                  nextPromise success responseArray(index + 1)
                }
              } else if (response startsWith ":") { // integer reply : lpush
                val nextPromise = promiseQueue.dequeue.asInstanceOf[Promise[Int]]
                val retInt = (response drop 1).toInt
                nextPromise success (if (retInt >= 0) retInt else -1)
              } else if (response startsWith "*") { // multi-bulk : lrange
                val nos = (response drop 1).toInt
                val elems = responseArray.zipWithIndex.filter(_._2 % 2 == 0).map(_._1).drop(1).toList
                val nextPromise = promiseQueue.dequeue.asInstanceOf[Promise[List[_]]]
                if (nos >= 0) nextPromise success elems
                else nextPromise failure (new KeyNotFoundException)
                break
              }
            }
          }

        case CommandFailed(w: Write) => println("fatal")
        case "close" => connection ! Close
        case _: ConnectionClosed => context stop self
      }
  }
  def sendRedisCommand(conn: ActorRef, data: ByteString, resultPromise: Promise[_])(implicit ec: ExecutionContext) = {
    conn ! Write(data)
    promiseQueue += resultPromise
    val f = resultPromise.future
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
      (key, value, (client ? ByteString(Set(key, value).line, "UTF-8")).mapTo[Boolean])
    }

    writeResults foreach { case (key, value, result) =>
      result onSuccess {
        case someBoolean if someBoolean == true => println("Set " + key + " to value " + value)
        case _ => println("Failed to set key " + key + " to value " + value)
      }
      result onFailure {
        case t => println("Failed to set key " + key + " to value " + value + " : " + t)
      }
    }

    // get the ones set earlier
    val readResults = keys map { key => (key, client.ask(ByteString(Get[String](key).line, "UTF-8")).mapTo[String]) }
    readResults zip values foreach { case ((key, result), expectedValue) =>
      result.onSuccess {
        case resultString =>
          println("Got a result for " + key + ": "+ resultString)
          assert(resultString == expectedValue)
      }
      result.onFailure {
        case t => println("Got some exception " + t)
      }
    }

    // lpush a bunch of stuff
    val forpush = List.fill(10)("listk") zip (1 to 10).map(_.toString)
    val writeListResults = forpush map { case (key, value) =>
      (key, value, (client ? ByteString(LPush(key, value).line, "UTF-8")).mapTo[Int])
    }

    writeListResults foreach { case (key, value, result) =>
      result onSuccess {
        case someInt: Int if someInt > 0 => println("Set " + key + " to value " + value + " and returned " + someInt)
        case _ => println("Failed to set key " + key + " to value " + value)
      }
      result onFailure {
        case t => println("Failed to set key " + key + " to value " + value + " : " + t)
      }
    }
    writeListResults.map(e => Await.result(e._3, 3 seconds))

    // do an lrange to check if they were inserted correctly & in proper order
    val readListResult = client.ask(ByteString(LRange[String]("listk", 0, -1).line, "UTF-8")).mapTo[List[String]]
    readListResult.onSuccess {
      case result =>
        println("Range = " + result)
        assert(result.map(_.toInt) == List(10, 9, 8, 7, 6, 5, 4, 3, 2, 1))
    }
    readListResult.onFailure {
      case t => println("Got some exception " + t)
    }

    // rpush a bunch of stuff
    val forrpush = List.fill(10)("listr") zip (1 to 10).map(_.toString)
    val writeListRes = forrpush map { case (key, value) =>
      (key, value, (client ? RPush(key, value)).mapTo[Int])
    }
    writeListRes.map(e => Await.result(e._3, 3 seconds))

    // do an lrange to check if they were inserted correctly & in proper order
    val readListRes = client.ask(ByteString(LRange[String]("listr", 0, -1).line, "UTF-8")).mapTo[List[String]]
    readListRes.onSuccess {
      case result =>
        println("Range = " + result)
        assert(result.map(_.toInt).reverse == List(10, 9, 8, 7, 6, 5, 4, 3, 2, 1))
    }
    readListRes.onFailure {
      case t => println("Got some exception " + t)
    }

    // implicit val parseInt = Parse[Int](new String(_).toInt)
  }
}
