package com.redis.akka

import java.net.InetSocketAddress
import scala.concurrent.{ExecutionContext, Await}
import scala.collection.immutable.Queue
import scala.concurrent.duration._
import akka.pattern.{ask, pipe}
import akka.io.{Tcp, IO}
import Tcp._
import akka.util.{ByteString, Timeout}
import akka.actor._
import ExecutionContext.Implicits.global
import scala.language.existentials

import ProtocolUtils._
import RedisCommands._
import RedisReplies._

class RedisClient(remote: InetSocketAddress) extends Actor {
  import Tcp._
  import context.system

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
          val replies = splitReplies(responseArray)
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
