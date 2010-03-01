package com.redis

trait SortedSetOperations { self: RedisClient =>
  import self._
  
  // ZADD
  // Add the specified member having the specified score to the sorted set stored at key.
  def zadd(key: String, score: String, member: String): Option[Int] = {
    send("ZADD", key, score, member)
    asInt
  }
  
  // ZREM
  // Remove the specified member from the sorted set value stored at key.
  def zrem(key: String, member: String): Option[Int] = {
    send("ZREM", key, member)
    asInt
  }
  
  // ZINCRBY
  // 
  def zincrby(key: String, incr: String, member: String): Option[Int] = {
    send("ZINCRBY", key, incr, member)
    asInt
  }
  
  // ZCARD
  // 
  def zcard(key: String): Option[Int] = {
    send("ZCARD", key)
    asInt
  }
  
  // ZSCORE
  // 
  def zscore(key: String, element: String): Option[String] = {
    send("ZSCORE", key, element)
    asString
  }
  
  // ZRANGE
  // 
  import Commands._
  import RedisClient._

  def zrange(key: String, start: String, end: String, sortAs: SortOrder, withScores: Boolean ): Option[List[Option[String]]] = {
    val command =
      sortAs match {
        case ASC =>
          cmd("ZRANGE", key, start, end)
        case DESC =>
          cmd("ZREVRANGE", key, start, end)
      }
    withScores match {
      case true =>
        send(command + "WITHSCORES" + Commands.LS)
      case false => 
        send(command)
    }
    asList
  }

  // ZRANGEBYSCORE
  // 
  def zrangebyscore(key: String, min: String, max: String, limit: Option[(String, String)]): Option[List[Option[String]]] = limit match {
    case None =>
      send("ZRANGEBYSCORE", key, min, max)
      asList
    case Some(l) =>
      send("ZRANGEBYSCORE", key, min, max, "LIMIT", l._1, l._2)
      asList
  }
}
