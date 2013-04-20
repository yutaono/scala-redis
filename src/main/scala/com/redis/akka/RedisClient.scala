package com.redis.akka

import java.net.InetSocketAddress
import scala.concurrent.{Promise, ExecutionContext}
import scala.collection.immutable.Queue
import scala.concurrent.duration._
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
          println("got set: " + data.decodeString("UTF-8"))
          sendRedisCommand(connection, data, Promise[Boolean]) 

        case data: ByteString if data.decodeString("UTF-8").startsWith("*2") => // get command 
          println("got get: " + data.decodeString("UTF-8"))
          sendRedisCommand(connection, data, Promise[String]) 

        case Received(data) => // redis replies 
          val r = data.decodeString("UTF-8")
          val responseArray = r.split("\r\n")
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
  }
}
