package com.redis.cluster

import org.scalatest.Spec
import org.scalatest.BeforeAndAfterEach
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith


@RunWith(classOf[JUnitRunner])
class RedisClusterSpec extends Spec 
                       with ShouldMatchers
                       with BeforeAndAfterEach
                       with BeforeAndAfterAll {

  val r = new RedisCluster("localhost:6379", "localhost:6380", "localhost:6381") {
    val keyTag = Some(RegexKeyTag)
  }

  override def beforeEach = {
  }

  override def afterEach = {
    r.flushdb
  }

  override def afterAll = {
    r.flushdb
  }

  describe("cluster operations") {
    it("should set") {
      val l = List("debasish", "maulindu", "ramanendu", "nilanjan", "tarun", "tarun", "tarun")

      // last 3 should map to the same node
      l.map(r.nodeForKey(_)).reverse.slice(0, 3).forall(_.toString == "localhost:6381") should equal(true)

      // set
      l.foreach{s =>
        val n = r.nodeForKey(s)
        r.set(s, "working in anshin") should equal(true)
      }

      // check get: should return all 5
      r.keys("*").get.size should equal(5)
    }

    it("should get keys from proper nodes") {
      val l = List("debasish", "maulindu", "ramanendu", "nilanjan", "tarun", "tarun", "tarun")

      // set
      l.foreach {s =>
        val n = r.nodeForKey(s)
        r.set(s, s + " is working in anshin") should equal(true)
      }

      r.get("debasish").get should equal("debasish is working in anshin")
      r.get("maulindu").get should equal("maulindu is working in anshin")
      l.map(r.get(_).get).removeDuplicates.size should equal(5)
    }

    it("should do all operations on the cluster") {
      val l = List("debasish", "maulindu", "ramanendu", "nilanjan", "tarun", "tarun", "tarun")

      // set
      l.foreach {s =>
        val n = r.nodeForKey(s)
        r.set(s, s + " is working in anshin") should equal(true)
      }

      r.dbsize.get should equal(5)
      r.exists("debasish") should equal(true)
      r.exists("maulindu") should equal(true)
      r.exists("debasish-1") should equal(false)

      r.del("debasish", "nilanjan").get should equal(2)
      r.dbsize.get should equal(3)
      r.del("satire").isDefined should equal(false)
    }

    it("mget on a cluster should fetch values in the same order as the keys") {
      val l = List("debasish", "maulindu", "ramanendu", "nilanjan", "tarun", "tarun", "tarun")

      // set
      l.foreach {s =>
        val n = r.nodeForKey(s)
        r.set(s, s + " is working in anshin") should equal(true)
      }

      // mget
      r.mget(l.head, l.tail: _*).get.map(_.get.split(" ")(0)) should equal(l)
    }

    it("list operations should work on the cluster"){
      r.lpush("java-virtual-machine-langs", "java") should equal(true)
      r.lpush("java-virtual-machine-langs", "jruby") should equal(true)
      r.lpush("java-virtual-machine-langs", "groovy") should equal(true)
      r.lpush("java-virtual-machine-langs", "scala") should equal(true)
      r.llen("java-virtual-machine-langs") should equal(Some(4))
    }

    it("keytags should ensure mapping to the same server"){
      r.lpush("java-virtual-machine-{langs}", "java") should equal(true)
      r.lpush("java-virtual-machine-{langs}", "jruby") should equal(true)
      r.lpush("java-virtual-machine-{langs}", "groovy") should equal(true)
      r.lpush("java-virtual-machine-{langs}", "scala") should equal(true)
      r.llen("java-virtual-machine-{langs}") should equal(Some(4))
      r.lpush("microsoft-platform-{langs}", "c++") should equal(true)
      r.rpoplpush("java-virtual-machine-{langs}", "microsoft-platform-{langs}").get should equal("java")
      r.llen("java-virtual-machine-{langs}") should equal(Some(3))
      r.llen("microsoft-platform-{langs}") should equal(Some(2))
    }
  }
}
