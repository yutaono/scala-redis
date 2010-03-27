package com.redis.ds

import org.scalatest.Spec
import org.scalatest.BeforeAndAfterEach
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith


@RunWith(classOf[JUnitRunner])
class DequeSpec extends Spec 
                with ShouldMatchers
                with BeforeAndAfterEach
                with BeforeAndAfterAll {

  val r = new RedisDequeClient("localhost", 6379).mkDeque("td")

  override def beforeEach = {
  }

  override def afterEach = {
    r.clear
  }

  override def afterAll = {
  }

  describe("addFirst") {
    it("should add to the head of the deque") {
      r.addFirst("foo") should equal(true)
      r.peekFirst should equal(Some("foo"))
      r.addFirst("bar") should equal(true)
      r.isEmpty should equal(false)
      r.peekFirst should equal(Some("bar"))
      r.clear should equal(true)
      r.size should equal(0)
      r.isEmpty should equal(true)
    }
  }
}
