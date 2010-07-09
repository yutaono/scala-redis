package com.redis

import org.scalatest.Spec
import org.scalatest.BeforeAndAfterEach
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith


@RunWith(classOf[JUnitRunner])
class SortedSetOperationsSpec extends Spec 
                        with ShouldMatchers
                        with BeforeAndAfterEach
                        with BeforeAndAfterAll {

  val r = new RedisClient("localhost", 6379)

  override def beforeEach = {
  }

  override def afterEach = {
    r.flushdb
  }

  override def afterAll = {
    r.disconnect
  }

  import r._

  private def add = {
    zadd("hackers", "1965", "yukihiro matsumoto").get should equal(1)
    zadd("hackers", "1953", "richard stallman").get should equal(1)
    zadd("hackers", "1916", "claude shannon").get should equal(1)
    zadd("hackers", "1969", "linus torvalds").get should equal(1)
    zadd("hackers", "1940", "alan kay").get should equal(1)
    zadd("hackers", "1912", "alan turing").get should equal(1)
  }

  describe("zadd") {
    it("should add based on proper sorted set semantics") {
      add
      zadd("hackers", "1912", "alan turing").get should equal(0)
      zcard("hackers").get should equal(6)
    }
  }

  describe("zrange") {
    it("should get the proper range") {
      add
      zrange("hackers", "0", "-1").get should have size (6)
      zrange("hackers", "0", "-1", withScores = true).get should have size(12)
      zrangeWithScore("hackers", "0", "-1").get should have size(6)
    }
  }

  describe("zrank") {
    it ("should give proper rank") {
      add
      zrank("hackers", "yukihiro matsumoto") should equal(Some(4))
      zrank("hackers", "yukihiro matsumoto", true) should equal(Some(1))
    }
  }

  describe("zremrangebyrank") {
    it ("should remove based on rank range") {
      add
      zremrangebyrank("hackers", 0, 2) should equal(Some(3))
    }
  }

  describe("zremrangebyscore") {
    it ("should remove based on score range") {
      add
      zremrangebyscore("hackers", 1912, 1940) should equal(Some(3))
      zremrangebyscore("hackers", 0, 3) should equal(Some(0))
    }
  }

  describe("zunion") {
    it ("should do a union") {
      zadd("hackers 1", "1965", "yukihiro matsumoto").get should equal(1)
      zadd("hackers 1", "1953", "richard stallman").get should equal(1)
      zadd("hackers 2", "1916", "claude shannon").get should equal(1)
      zadd("hackers 2", "1969", "linus torvalds").get should equal(1)
      zadd("hackers 3", "1940", "alan kay").get should equal(1)
      zadd("hackers 4", "1912", "alan turing").get should equal(1)

      // union with weight = 1
      zunion("hackers", 4, List("hackers 1", "hackers 2", "hackers 3", "hackers 4")) should equal(Some(6))
      zcard("hackers") should equal(Some(6))

      zrangeWithScore("hackers", "0", "-1").get.map(_._2.get.toInt) should equal(List(1912, 1916, 1940, 1953, 1965, 1969))

      // union with modified weights
      zunion("hackers weighted", 4, List("hackers 1", "hackers 2", "hackers 3", "hackers 4"), List(1, 2, 3, 4)) should equal(Some(6))
      zrangeWithScore("hackers weighted", "0", "-1").get.map(_._2.get.toInt) should equal(List(1953, 1965, 3832, 3938, 5820, 7648))
    }
  }
}
