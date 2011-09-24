package com.redis

import org.scalatest.Spec
import org.scalatest.BeforeAndAfterEach
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import Patterns._

@RunWith(classOf[JUnitRunner])
class PatternsSpec extends Spec 
               with ShouldMatchers
               with BeforeAndAfterEach
               with BeforeAndAfterAll {

  implicit val clients = new RedisClientPool("localhost", 6379)

  override def beforeEach = {
  }

  override def afterEach = clients.withClient{
    client => client.flushdb
  }

  override def afterAll = {
    clients.withClient{ client => client.disconnect }
    clients.close
  }

  def runScatterGather(opsPerRun: Int) = {
    val start = System.nanoTime
    scatterGatherWithList(opsPerRun)
    val elapsed: Double = (System.nanoTime - start) / 1000000000.0
    val opsPerSec: Double = (100 * opsPerRun * 2) / elapsed
    println("Operations per run: " + opsPerRun * 100 * 2 + " elapsed: " + elapsed + " ops per second: " + opsPerSec)
  }

  describe("scatter/gather with list test 1") {
    it("should distribute work amongst the clients") {
      runScatterGather(2000)
    }
  }

  describe("scatter/gather with list test 2") {
    it("should distribute work amongst the clients") {
      runScatterGather(5000)
    }
  }

  describe("scatter/gather with list test 3") {
    it("should distribute work amongst the clients") {
      runScatterGather(10000)
    }
  }
}
