package com.redis.operations

import com.redis.SocketOperations._

trait SortedSetOperations {
  val connection: Connection
  
  import connection._
  
  // ZADD
  // Add the specified member having the specified score to the sorted set stored at key.
  def zAdd(key: String, score: String, member: String): Boolean = {
    write(multiBulkCommand("ZADD", key, score, member))
    readBoolean
  }
  
  // ZREM
  // Remove the specified member from the sorted set value stored at key.
  def zRem(key: String, member: String): Boolean = {
    write(multiBulkCommand("ZREM", key, member))
    readBoolean
  }
  
  // ZINCRBY
  // 
  def zIncrBy(key: String, incr: String, member: String): Option[Int] = {
    write(multiBulkCommand("ZINCRBY", key, incr, member))
    readInt
  }
  
  // ZCARD
  // 
  def zCard(key: String): Option[Int] = {
    write(multiBulkCommand("ZCARD", key))
    readInt
  }
  
  // ZSCORE
  // 
  def zScore(key: String, element: String): Option[String] = {
    write(multiBulkCommand("ZSCORE", key, element))
    readString
  }
  
  // ZRANGE
  // 
  def zRange(key: String, start: String, end: String, sortAs: SortOrder, withScores: Boolean ): Option[List[String]] = {
    val command =
      sortAs match {
        case ASC =>
          multiBulkCommand("ZRANGE", key, start, end)
        case DESC =>
          multiBulkCommand("ZREVRANGE", key, start, end)
      }
    withScores match {
      case true =>
        write(command + "WITHSCORES" + LS)
      case false => 
        write(command)
    }
    readList
  }

  // ZRANGEBYSCORE
  // 
  def zRangeByScore(key: String, min: String, max: String, limit: Option[(String, String)]): Option[List[String]] = limit match {
    case None =>
      write(multiBulkCommand("ZRANGEBYSCORE", key, min, max))
      readList
    case Some(l) =>
      write(multiBulkCommand("ZRANGEBYSCORE", key, min, max, "LIMIT", l._1, l._2))
      readList
  }
}
