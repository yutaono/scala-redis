package com.redis

import org.scalatest.Spec
import org.scalatest.BeforeAndAfterEach
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import scala.actors._
import scala.actors.Actor._

@RunWith(classOf[JUnitRunner])
class PoolSpec extends Spec 
               with ShouldMatchers
               with BeforeAndAfterEach
               with BeforeAndAfterAll {

  val clients = new RedisClientPool("localhost", 16379)

  override def beforeEach = {
  }

  override def afterEach = clients.withClient{
    client => client.flushdb
  }

  override def afterAll = clients.withClient{
    client => client.disconnect
  }

  def lp(msgs: List[String]) = {
    clients.withClient {
      client => {
        msgs.foreach(client.lpush("list-l", _))
        client.llen("list-l")
      }
    }
  }

  def rp(msgs: List[String]) = {
    clients.withClient {
      client => {
        msgs.foreach(client.rpush("list-r", _))
        client.llen("list-r")
      }
    }
  }

  def set(msgs: List[String]) = {
    clients.withClient {
      client => {
        var i = 0
        msgs.foreach { v =>
          client.set("key-%d".format(i), v)
          i += 1
        }
        Some(1000)
      }
    }
  }

  describe("pool test") {
    it("should distribute work amongst the clients") {
      val l = (0 until 5000).map(_.toString).toList
      val fns = List[List[String] => Option[Int]](lp, rp, set)
      val tasks = fns map (fn => scala.actors.Futures.future { fn(l) })
      val results = tasks map (future => future.apply())
      results should equal(List(Some(5000), Some(5000), Some(1000)))
    }
  }
}
