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
      zrange("hackers", "0", "-1", RedisClient.ASC, false).get should have size (6)

        // should equal(Some(List(Some("alan turing"), Some("claude shannon"), Some("alan kay"), Some("richard stallman"), Some("yukihiro matsumoto"), Some("linus torvalds"))))

      zrange("hackers", "0", "-1", RedisClient.ASC, true).get should have size(12)
        
        // should equal(Some(List(Some("alan turing"), Some(1912), Some("claude shannon"), Some(1916), Some("alan kay"), Some(1940), Some("richard stallman"), Some(1953), Some("yukihiro matsumoto"), Some(1965), Some("linus torvalds"), Some(1969))))

      zrangeWithScore("hackers", "0", "-1", RedisClient.ASC).get should have size(6)
        
        // should equal (Some(List((Some("alan turing"),Some(1912)), (Some("claude shannon"),Some(1916)), (Some("alan kay"),Some(1940)), (Some("richard stallman"),Some(1953)), (Some("yukihiro matsumoto"),Some(1965)), (Some("linus torvalds"),Some(1969)))))
    }
  }
}
