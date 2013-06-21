package com.redis.nonblocking

import java.net.InetSocketAddress
import scala.concurrent.{ExecutionContext, Await}
import scala.concurrent.duration._
import akka.pattern.{ask, pipe}
import akka.event.Logging
import akka.util.Timeout
import akka.actor._
import ExecutionContext.Implicits.global

import StringCommands._
import ListCommands._

import StringOperations._

import org.scalatest.FunSpec
import org.scalatest.BeforeAndAfterEach
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith


@RunWith(classOf[JUnitRunner])
class ClientSpec extends FunSpec 
                 with ShouldMatchers
                 with BeforeAndAfterEach
                 with BeforeAndAfterAll {

  implicit val system = ActorSystem("redis-client")

  val endpoint = new InetSocketAddress("localhost", 6379)
  val client = system.actorOf(Props(new RedisClient(endpoint)), name = "redis-client")
  implicit val timeout = Timeout(5 seconds)
  Thread.sleep(2000)


  override def beforeEach = {
  }

  override def afterEach = {
  }

  override def afterAll = {
    client ! "close"
  }

  describe("set") {
    it("should set values to keys") {
      val numKeys = 3
      val (keys, values) = (1 to numKeys map { num => ("key" + num, "value" + num) }).unzip
      val writes = keys zip values map { case (key, value) => set(key, value) apply client }

      writes foreach { _ onSuccess {
        case Some(r) => r should equal(true)
        case _ => fail("set should pass")
      }}
    }
  }

  describe("get") {
    it("should get results for keys set earlier") {
      val numKeys = 3
      val (keys, values) = (1 to numKeys map { num => ("key" + num, "value" + num) }).unzip
      val reads = keys map { key => get(key) apply client }

      reads zip values foreach { case (result, expectedValue) =>
        result.onSuccess {
          case resultString => resultString should equal(Some(expectedValue))
        }
        result.onFailure {
          case t => println("Got some exception " + t)
        }
      }
      reads.map(e => Await.result(e, 3 seconds)) should equal(List(Some("value1"), Some("value2"), Some("value3")))
    }
    it("should give none for unknown keys") {
      val reads = get("key10") apply client
      Await.result(reads, 3 seconds) should equal(None)
    }
  }

  describe("using api") {
    set("kolkata", "beautiful") apply (client) onSuccess {
      case Some(r) => r should equal(true)
      case _ => fail("set should pass")
    }

    ((set("debasish", "scala") apply client) flatMap (x => get("debasish") apply client)) onSuccess {
      case Some(r) => r should equal("scala")
      case _ => fail("should get")
    }
  }

  describe("lpush") {
    it("should do an lpush and retrieve the values using lrange") {
      val forpush = List.fill(10)("listk") zip (1 to 10).map(_.toString)

      val writeListResults = forpush map { case (key, value) =>
        (key, value, (client ? LPush(key, value)).mapTo[Option[Long]])
      }

      writeListResults foreach { case (key, value, result) =>
        result onSuccess {
          case Some(someLong) if someLong > 0 => {
            someLong should (be > (0L) and be <= (10L))
          }
          case _ => fail("lpush must return a positive number")
        }
      }
      writeListResults.map(e => Await.result(e._3, 3 seconds)) should equal((1 to 10).map(e => Some(e)).toList)

      // do an lrange to check if they were inserted correctly & in proper order
      val readListResult = client.ask(LRange[String]("listk", 0, -1)).mapTo[Option[List[String]]]
      readListResult.onSuccess {
        case result => result.get should equal ((1 to 10).reverse.map(e => Some(e.toString)).toList)
      }
    }
  }

  describe("rpush") {
    it("should do an rpush and retrieve the values using lrange") {
      val forrpush = List.fill(10)("listr") zip (1 to 10).map(_.toString)
      val writeListRes = forrpush map { case (key, value) =>
        (key, value, (client ? RPush(key, value)).mapTo[Option[Long]])
      }
      writeListRes.map(e => Await.result(e._3, 3 seconds)) should equal((1 to 10).map(e => Some(e)).toList)

      // do an lrange to check if they were inserted correctly & in proper order
      val readListRes = client.ask(LRange[String]("listr", 0, -1)).mapTo[Option[List[String]]]
      readListRes.onSuccess {
        case result => result.get.reverse should equal ((1 to 10).reverse.map(e => Some(e.toString)).toList)
      }
    }
  }
    
}
