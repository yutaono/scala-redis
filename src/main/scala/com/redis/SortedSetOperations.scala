package com.redis

trait SortedSetOperations { self: Redis =>
  
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

  def zrange(key: String, start: String, end: String, sortAs: SortOrder = ASC, withScores: Boolean = false ): Option[List[Option[String]]] = {
    val sort =
      sortAs match {
        case ASC => "ZRANGE"
        case _ => "ZREVRANGE"
      }
    val command = 
      withScores match {
        case true => cmd(sort, key, start, end, "WITHSCORES")
        case _ => cmd(sort, key, start, end)
      }
    send(command)
    asList
  }

  def zrangeWithScore(key: String, start: String, end: String, sortAs: SortOrder = ASC): Option[List[(Option[String], Option[String])]] = {
    zrange(key, start, end, sortAs, true) match {
      case None => None
      case Some(l) => Some(makeTuple(l))
    }
  }

  private def makeTuple(l: List[Option[String]]): List[(Option[String], Option[String])] = l match {
    case List() => List()
    case a :: b :: rest => (a, b) :: makeTuple(rest)
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

  // ZRANK
  // ZREVRANK
  //
  def zrank(key: String, member: String, reverse: Boolean = false) = reverse match {
    case false =>
      send("ZRANK", key, member)
      asInt
    case _ =>
      send("ZREVRANK", key, member)
      asInt
  }

  // ZREMRANGEBYRANK
  //
  def zremrangebyrank(key: String, start: Int, end: Int) = {
    send("ZREMRANGEBYRANK", key, String.valueOf(start), String.valueOf(end))
    asInt
  }

  // ZREMRANGEBYSCORE
  //
  def zremrangebyscore(key: String, start: Int, end: Int) = {
    send("ZREMRANGEBYSCORE", key, String.valueOf(start), String.valueOf(end))
    asInt
  }

  // ZUNION
  //
  def zunion(dstKey: String, noOfKeys: Int, keys: List[String], weights: List[Int] = List[Int]()) = weights match {
    case List() =>
      send("ZUNIONSTORE", dstKey, (String.valueOf(noOfKeys) :: keys):_*)
      asInt
    case _ =>
      send("ZUNIONSTORE", 
        dstKey, 
        ((String.valueOf(noOfKeys) :: keys) ::: ("WEIGHTS" :: weights.map(String.valueOf(_)))):_*)
      asInt
  }
}
