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

// example adopted from : http://rajivkurian.github.io/blog/2012/12/25/a-simple-redis-client-using-spray-io-actors-futures-and-promises/
object RedisCommands {
  sealed trait RedisCommand {
    def line: String
  }

  case class Get(key: String) extends RedisCommand {
    val line = "*2\r\n" +
               "$3\r\n" +
               "GET\r\n" +
               "$" + key.length + "\r\n" +
               key + "\r\n"
  }
  case class Set(key: String, value: String) extends RedisCommand {
    val line = "*3\r\n" +
               "$3\r\n" +
               "SET\r\n" +
               "$" + key.length + "\r\n" +
               key + "\r\n" +
               "$" + value.length + "\r\n" +
               value + "\r\n"
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
  case class LRange(key: String, start: Int, stop: Int) extends RedisCommand {
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
        case data: ByteString if data.decodeString("UTF-8").startsWith("*3") => // set command 
          println("got set or lrange: " + data.decodeString("UTF-8"))
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
    val readResults = keys map { key => (key, client.ask(ByteString(Get(key).line, "UTF-8")).mapTo[String]) }
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
    val readListResult = client.ask(ByteString(LRange("listk", 0, -1).line, "UTF-8")).mapTo[List[String]]
    readListResult.onSuccess {
      case result =>
        println("Range = " + result)
        assert(result.map(_.toInt) == List(10, 9, 8, 7, 6, 5, 4, 3, 2, 1))
    }
    readListResult.onFailure {
      case t => println("Got some exception " + t)
    }
  }
}
