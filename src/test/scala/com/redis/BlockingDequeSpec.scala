package com.redis.ds

import org.scalatest.Spec
import org.scalatest.BeforeAndAfterEach
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith


@RunWith(classOf[JUnitRunner])
class BlockingDequeSpec extends Spec 
                with ShouldMatchers
                with BeforeAndAfterEach
                with BeforeAndAfterAll {

  val r1 = new RedisDequeClient("localhost", 6379).getDeque("btd", true, 30)
  val r2 = new RedisDequeClient("localhost", 6379).getDeque("btd", true, 30)

  override def beforeEach = {
    r1.clear
  }

  override def afterEach = {
  }

  override def afterAll = {
  }

  describe("blocking poll") {
    it("should pull out first element") {

      class Foo extends Runnable {
        def start () {
          val myThread = new Thread(this) ;
          myThread.start() ;
        }

        def run {
          val v = r1.poll
          v.get should equal("foo")
        }
      }
      (new Foo).start
      r2.size should equal(0)
      r2.addFirst("foo")
    }
  }
}
