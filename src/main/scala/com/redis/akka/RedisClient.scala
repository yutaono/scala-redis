package com.redis.nonblocking

import java.net.InetSocketAddress
import scala.concurrent.{ExecutionContext, Await}
import scala.collection.immutable.Queue
import scala.concurrent.duration._
import akka.pattern.{ask, pipe}
import akka.event.Logging
import akka.io.{Tcp, IO}
import Tcp._
import akka.util.{ByteString, Timeout}
import akka.actor._
import ExecutionContext.Implicits.global
import scala.language.existentials

import ProtocolUtils._
import RedisReplies._

class RedisClient(remote: InetSocketAddress) extends Actor {
  import Tcp._
  import context.system

  val log = Logging(context.system, this)
  var buffer = Vector.empty[RedisCommand]
  val promiseQueue = new scala.collection.mutable.Queue[RedisCommand]
  IO(Tcp) ! Connect(remote)

  def receive = baseHandler

  def baseHandler: Receive = {
    case CommandFailed(c: Connect) =>
      log.error("Connect failed for " + c.remoteAddress + " with " + c.failureMessage + " stopping ..")
      context stop self

    case c@Connected(remote, local) =>
      log.info(c.toString)
      val connection = sender
      connection ! Register(self)

      context become {
        case command: RedisCommand => 
          log.info("sending command to Redis: " + command)
          if (!buffer.isEmpty) buffer.foreach(c => sendRedisCommand(connection, c))
          else sendRedisCommand(connection, command)

        case Received(data) => 
          log.info("got response from Redis: " + data.decodeString("UTF-8"))
          val responseArray = data.toArray[Byte]
          val replies = splitReplies(responseArray)
          replies.map {r =>
            promiseQueue.dequeue reply r
          }

        case CommandFailed(w: Write) => {
          log.error("Write failed for " + w.data.decodeString("UTF-8"))
          connection ! ResumeWriting
          context become buffering(connection)
        }
        case "close" => {
          log.info("Got to close ..")
          if (!buffer.isEmpty) buffer.foreach(c => sendRedisCommand(connection, c))
          connection ! Close
        }
        case c: ConnectionClosed => {
          log.info("stopping ..")
          log.info("error cause = " + c.getErrorCause + " isAborted = " + c.isAborted + " isConfirmed = " + c.isConfirmed + " isErrorClosed = " + c.isErrorClosed + " isPeerClosed = " + c.isPeerClosed)
          context stop self
        }
      }
  }

  def buffering(conn: ActorRef): Receive = {
    var peerClosed = false
    var pingAttempts = 0

    {
      case command: RedisCommand => {
        buffer :+= command 
        pingAttempts += 1
        if (pingAttempts == 10) {
          log.info("Can't recover .. closing and discarding buffered commands")
          conn ! Close
          context stop self
        }
      }
      case PeerClosed => peerClosed = true
      case WritingResumed => {
        if (peerClosed) {
          log.info("Can't recover .. closing and discarding buffered commands")
          conn ! Close
          context stop self
        } else {
          context become baseHandler
        }
      }
    }
  }

  def sendRedisCommand(conn: ActorRef, command: RedisCommand)(implicit ec: ExecutionContext) = {
    promiseQueue += command
    if (!buffer.isEmpty) buffer = buffer drop 1
    conn ! Write(ByteString(command.line))
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
        case t: Exception => println("Failure: Failed to set key " + key + " to value " + value + " : " + t)
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
    client ! "close"
  }
}
