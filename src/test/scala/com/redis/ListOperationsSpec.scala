package com.redis

import org.scalatest.Spec
import org.scalatest.BeforeAndAfterEach
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith


@RunWith(classOf[JUnitRunner])
class ListOperationsSpec extends Spec 
                         with ShouldMatchers
                         with BeforeAndAfterEach
                         with BeforeAndAfterAll {

  val r = new RedisClient("localhost", 6379)

  override def beforeEach = {
  }

  override def afterEach = {
    r.flushDb
  }

  override def afterAll = {
    r.disconnect
  }

  describe("lpush") {
    it("should add to the head of the list") {
      r.lpush("list-1", "foo") should equal(true)
      r.lpush("list-1", "bar") should equal(true)
    }
    it("should throw if the key has a non-list value") {
      r.set("anshin-1", "debasish") should equal(true)
      val thrown = evaluating { r.lpush("anshin-1", "bar") } should produce [Exception]
      thrown.getMessage should equal("ERR Operation against a key holding the wrong kind of value")
    }
  }

  describe("rpush") {
    it("should add to the head of the list") {
      r.rpush("list-1", "foo") should equal(true)
      r.rpush("list-1", "bar") should equal(true)
    }
    it("should throw if the key has a non-list value") {
      r.set("anshin-1", "debasish") should equal(true)
      val thrown = evaluating { r.rpush("anshin-1", "bar") } should produce [Exception]
      thrown.getMessage should equal("ERR Operation against a key holding the wrong kind of value")
    }
  }

  describe("llen") {
    it("should return the length of the list") {
      r.lpush("list-1", "foo") should equal(true)
      r.lpush("list-1", "bar") should equal(true)
      r.llen("list-1").get should equal(2)
    }
    it("should return 0 for a non-existent key") {
      r.llen("list-2").get should equal(0)
    }
    it("should throw for a non-list key") {
      r.set("anshin-1", "debasish") should equal(true)
      val thrown = evaluating { r.llen("anshin-1") } should produce [Exception]
      thrown.getMessage should equal("ERR Operation against a key holding the wrong kind of value")
    }
  }

  describe("lrange") {
    it("should return the range") {
      r.lpush("list-1", "6") should equal(true)
      r.lpush("list-1", "5") should equal(true)
      r.lpush("list-1", "4") should equal(true)
      r.lpush("list-1", "3") should equal(true)
      r.lpush("list-1", "2") should equal(true)
      r.lpush("list-1", "1") should equal(true)
      r.llen("list-1").get should equal(6)
      r.lrange("list-1", 0, 4).get should equal(List(Some("1"), Some("2"), Some("3"), Some("4"), Some("5")))
    }
    it("should return empty list if start > end") {
      r.lpush("list-1", "3") should equal(true)
      r.lpush("list-1", "2") should equal(true)
      r.lpush("list-1", "1") should equal(true)
      r.lrange("list-1", 2, 0).get should equal(List())
    }
    it("should treat as end of list if end is over the actual end of list") {
      r.lpush("list-1", "3") should equal(true)
      r.lpush("list-1", "2") should equal(true)
      r.lpush("list-1", "1") should equal(true)
      r.lrange("list-1", 0, 7).get should equal(List(Some("1"), Some("2"), Some("3")))
    }
  }

  describe("ltrim") {
    it("should trim to the input size") {
      r.lpush("list-1", "6") should equal(true)
      r.lpush("list-1", "5") should equal(true)
      r.lpush("list-1", "4") should equal(true)
      r.lpush("list-1", "3") should equal(true)
      r.lpush("list-1", "2") should equal(true)
      r.lpush("list-1", "1") should equal(true)
      r.ltrim("list-1", 0, 3) should equal(true)
      r.llen("list-1") should equal(Some(4))
    }
    it("should should return empty list for start > end") {
      r.lpush("list-1", "6") should equal(true)
      r.lpush("list-1", "5") should equal(true)
      r.lpush("list-1", "4") should equal(true)
      r.ltrim("list-1", 6, 3) should equal(true)
      r.llen("list-1") should equal(Some(0))
    }
    it("should treat as end of list if end is over the actual end of list") {
      r.lpush("list-1", "6") should equal(true)
      r.lpush("list-1", "5") should equal(true)
      r.lpush("list-1", "4") should equal(true)
      r.ltrim("list-1", 0, 12) should equal(true)
      r.llen("list-1") should equal(Some(3))
    }
  }

  describe("lindex") {
    it("should return the value at index") {
      r.lpush("list-1", "6") should equal(true)
      r.lpush("list-1", "5") should equal(true)
      r.lpush("list-1", "4") should equal(true)
      r.lpush("list-1", "3") should equal(true)
      r.lpush("list-1", "2") should equal(true)
      r.lpush("list-1", "1") should equal(true)
      r.lindex("list-1", 2) should equal(Some("3"))
      r.lindex("list-1", 3) should equal(Some("4"))
      r.lindex("list-1", -1) should equal(Some("6"))
    }
    it("should return None if the key does not point to a list") {
      r.set("anshin-1", "debasish") should equal(true)
      r.lindex("list-1", 0) should equal(None)
    }
    it("should return empty string for an index out of range") {
      r.lpush("list-1", "6") should equal(true)
      r.lpush("list-1", "5") should equal(true)
      r.lpush("list-1", "4") should equal(true)
      r.lindex("list-1", 8) should equal(None) // the protocol says it will return empty string
    }
  }

  describe("lset") {
    it("should set value for key at index") {
      r.lpush("list-1", "6") should equal(true)
      r.lpush("list-1", "5") should equal(true)
      r.lpush("list-1", "4") should equal(true)
      r.lpush("list-1", "3") should equal(true)
      r.lpush("list-1", "2") should equal(true)
      r.lpush("list-1", "1") should equal(true)
      r.lset("list-1", 2, "30") should equal(true)
      r.lindex("list-1", 2) should equal(Some("30"))
    }
    it("should generate error for out of range index") {
      r.lpush("list-1", "6") should equal(true)
      r.lpush("list-1", "5") should equal(true)
      r.lpush("list-1", "4") should equal(true)
      val thrown = evaluating { r.lset("list-1", 12, "30") } should produce [Exception]
      thrown.getMessage should equal("ERR index out of range")
    }
  }

  describe("lrem") {
    it("should remove count elements matching value from beginning") {
      r.lpush("list-1", "6") should equal(true)
      r.lpush("list-1", "hello") should equal(true)
      r.lpush("list-1", "4") should equal(true)
      r.lpush("list-1", "hello") should equal(true)
      r.lpush("list-1", "hello") should equal(true)
      r.lpush("list-1", "hello") should equal(true)
      r.lrem("list-1", 2, "hello") should equal(Some(2))
      r.llen("list-1") should equal(Some(4))
    }
    it("should remove all elements matching value from beginning") {
      r.lpush("list-1", "6") should equal(true)
      r.lpush("list-1", "hello") should equal(true)
      r.lpush("list-1", "4") should equal(true)
      r.lpush("list-1", "hello") should equal(true)
      r.lpush("list-1", "hello") should equal(true)
      r.lpush("list-1", "hello") should equal(true)
      r.lrem("list-1", 0, "hello") should equal(Some(4))
      r.llen("list-1") should equal(Some(2))
    }
    it("should remove count elements matching value from end") {
      r.lpush("list-1", "6") should equal(true)
      r.lpush("list-1", "hello") should equal(true)
      r.lpush("list-1", "4") should equal(true)
      r.lpush("list-1", "hello") should equal(true)
      r.lpush("list-1", "hello") should equal(true)
      r.lpush("list-1", "hello") should equal(true)
      r.lrem("list-1", -2, "hello") should equal(Some(2))
      r.llen("list-1") should equal(Some(4))
      r.lindex("list-1", -2) should equal(Some("4"))
    }
  }

  describe("lpop") {
    it("should pop the first one from head") {
      r.lpush("list-1", "6") should equal(true)
      r.lpush("list-1", "5") should equal(true)
      r.lpush("list-1", "4") should equal(true)
      r.lpush("list-1", "3") should equal(true)
      r.lpush("list-1", "2") should equal(true)
      r.lpush("list-1", "1") should equal(true)
      r.lpop("list-1") should equal(Some("1"))
      r.lpop("list-1") should equal(Some("2"))
      r.lpop("list-1") should equal(Some("3"))
      r.llen("list-1") should equal(Some(3))
    }
    it("should give nil for non-existent key") {
      r.lpush("list-1", "6") should equal(true)
      r.lpush("list-1", "5") should equal(true)
      r.lpop("list-2") should equal(None)
      r.llen("list-1") should equal(Some(2))
    }
  }

  describe("rpop") {
    it("should pop the first one from tail") {
      r.lpush("list-1", "6") should equal(true)
      r.lpush("list-1", "5") should equal(true)
      r.lpush("list-1", "4") should equal(true)
      r.lpush("list-1", "3") should equal(true)
      r.lpush("list-1", "2") should equal(true)
      r.lpush("list-1", "1") should equal(true)
      r.rpop("list-1") should equal(Some("6"))
      r.rpop("list-1") should equal(Some("5"))
      r.rpop("list-1") should equal(Some("4"))
      r.llen("list-1") should equal(Some(3))
    }
    it("should give nil for non-existent key") {
      r.lpush("list-1", "6") should equal(true)
      r.lpush("list-1", "5") should equal(true)
      r.rpop("list-2") should equal(None)
      r.llen("list-1") should equal(Some(2))
    }
  }

  describe("rpoplpush") {
    it("should do") {
      r.rpush("list-1", "a") should equal(true)
      r.rpush("list-1", "b") should equal(true)
      r.rpush("list-1", "c") should equal(true)

      r.rpush("list-2", "foo") should equal(true)
      r.rpush("list-2", "bar") should equal(true)
      r.rpoplpush("list-1", "list-2") should equal(Some("c"))
      r.lindex("list-2", 0) should equal(Some("c"))
      r.llen("list-1") should equal(Some(2))
      r.llen("list-2") should equal(Some(3))
    }

    it("should rotate the list when src and dest are the same") {
      r.rpush("list-1", "a") should equal(true)
      r.rpush("list-1", "b") should equal(true)
      r.rpush("list-1", "c") should equal(true)
      r.rpoplpush("list-1", "list-1") should equal(Some("c"))
      r.lindex("list-1", 0) should equal(Some("c"))
      r.lindex("list-1", 2) should equal(Some("b"))
      r.llen("list-1") should equal(Some(3))
    }

    it("should give None for non-existent key") {
      r.rpoplpush("list-1", "list-2") should equal(None)
      r.rpush("list-1", "a") should equal(true)
      r.rpush("list-1", "b") should equal(true)
      r.rpoplpush("list-1", "list-2") should equal(Some("b"))
    }
  }

  /**
  describe("blpop") {
    it ("should do") {
      r.lpush("l1", "a") should equal(true)
      r.lpop("l1")
      r.llen("l1") should equal(Some(0))
    }
  }
**/
}
