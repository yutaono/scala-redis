package com.redis

import org.scalatest.Spec
import org.scalatest.BeforeAndAfterEach
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith


@RunWith(classOf[JUnitRunner])
class EvalOperationsSpec extends Spec
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

  describe("eval") {
    it("should eval lua code and get a string reply") {
      r.evalBulk[String]("return 'val1';", List(), List()) should be(Some("val1"))
    }

    it("should eval lua code and get a string array reply") {
      r.evalMultiBulk[String]("return { 'val1','val2' };", List(), List()) should be(Some(List(Some("val1"), Some("val2"))))
    }

    it("should eval lua code and get a string reply when passing keys") {
      r.set("a", "b")
      r.evalBulk[String]("return redis.call('get', KEYS[1]);", List("a"), List()) should be(Some("b"))
    }
  }
}
