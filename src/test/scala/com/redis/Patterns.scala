package com.redis

import serialization._
import java.util.concurrent.Executors
import com.twitter.util.{Future, FuturePool}

/**
 * Implementing some of the common patterns like scatter/gather, which can benefit from
 * a non-blocking asynchronous mode of operations. We use the scala-redis blocking client 
 * along with connection pools and future based execution. In this example we use the 
 * future implementation of Twitter Finagle (http://github.com/twitter/finagle).
 *
 * Some figures are also available for these patterns in the corresponding test suites.
 * The test suite for this pattern is in PatternsSpec.scala and the figures when run on
 * an MBP quad core 8G are:
 *
 * ---------------------------------------------------------------------------------------
 * Operations per run: 400000 elapsed: 3.279764 ops per second: 121959.99468254423
 * Operations per run: 1000000 elapsed: 7.746883 ops per second: 129084.17488685448
 * Operations per run: 2000000 elapsed: 15.391637 ops per second: 129940.69441736444
 * ---------------------------------------------------------------------------------------
 */
object Patterns {
  def listPush(count: Int, key: String)(implicit clients: RedisClientPool) = { 
    clients.withClient { client =>
      (1 to count) foreach {i => client.rpush(key, i)}
      assert(client.llen(key) == Some(count))
    }
    0
  }

  def listPop(count: Int, key: String)(implicit clients: RedisClientPool) = {
    implicit val parseInt = Parse[Int](new String(_).toInt)
    clients.withClient { client =>
      val list = (1 to count) map {i => client.lpop[Int](key).get}
      assert(client.llen(key) == Some(0))
      list.sum
    }
  }

  def scatterGatherWithList(opsPerClient: Int)(implicit clients: RedisClientPool) = {
    // set up Executors
    val futures = FuturePool(Executors.newFixedThreadPool(8))

    // scatter phase: push to 100 lists in parallel
    val futurePushes: Seq[Future[Int]] =
      (1 to 100) map {i => 
        futures {
          listPush(opsPerClient, "list_" + i)
        }
      }

    // wait till all pushes complete
    val allPushes: Future[Seq[Int]] = Future.collect(futurePushes)
    
    // entering gather phase
    val allSum = 
      allPushes onSuccess {result =>

        // pop from all 100 lists in parallel
        val futurePops: Seq[Future[Int]] =
          (1 to 100) map {i => 
            futures {
              listPop(opsPerClient, "list_" + i)
            }
          }

        // wait till all pops are complete
        val allPops: Future[Seq[Int]] = Future.collect(futurePops)

        // compute sum of all integers
        allPops onSuccess {members => members.sum}
      }
    allSum.apply
  }
}
