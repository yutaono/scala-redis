package com.redis

trait SetOperations { self: Redis =>

  // SADD
  // Add the specified member to the set value stored at key.
  def sadd(key: String, value: String): Option[Int] = {
    send("SADD", key, value)
    as[Int]
  }

  // SREM
  // Remove the specified member from the set value stored at key.
  def srem(key: String, value: String): Option[Int] = {
    send("SREM", key, value)
    as[Int]
  }

  // SPOP
  // Remove and return (pop) a random element from the Set value at key.
  def spop(key: String): Option[String] = {
    send("SPOP", key)
    as[String]
  }

  // SMOVE
  // Move the specified member from one Set to another atomically.
  def smove(sourceKey: String, destKey: String, value: String): Option[Int] = {
    send("SMOVE", sourceKey, destKey, value)
    as[Int]
  }

  // SCARD
  // Return the number of elements (the cardinality) of the Set at key.
  def scard(key: String): Option[Int] = {
    send("SCARD", key)
    as[Int]
  }

  // SISMEMBER
  // Test if the specified value is a member of the Set at key.
  def sismember(key: String, value: String): Boolean = {
    send("SISMEMBER", key, value)
    asBoolean
  }

  // SINTER
  // Return the intersection between the Sets stored at key1, key2, ..., keyN.
  def sinter(key: String, keys: String*): Option[Set[Option[String]]] = {
    send("SINTER", key, keys: _*)
    as[Set[Option[String]]]((x: Iterable[Option[String]]) => Set(x.toSeq: _*))
  }

  // SINTERSTORE
  // Compute the intersection between the Sets stored at key1, key2, ..., keyN, 
  // and store the resulting Set at dstkey.
  // SINTERSTORE returns the size of the intersection, unlike what the documentation says
  // refer http://code.google.com/p/redis/issues/detail?id=121
  def sinterstore(key: String, keys: String*): Option[Int] = {
    send("SINTERSTORE", key, keys: _*)
    as[Int]
  }

  // SUNION
  // Return the union between the Sets stored at key1, key2, ..., keyN.
  def sunion(key: String, keys: String*): Option[Set[Option[String]]] = {
    send("SUNION", key, keys: _*)
    as[Set[Option[String]]]((x: Iterable[Option[String]]) => Set(x.toSeq: _*))
  }

  // SUNIONSTORE
  // Compute the union between the Sets stored at key1, key2, ..., keyN, 
  // and store the resulting Set at dstkey.
  // SUNIONSTORE returns the size of the union, unlike what the documentation says
  // refer http://code.google.com/p/redis/issues/detail?id=121
  def sunionstore(key: String, keys: String*): Option[Int] = {
    send("SUNIONSTORE", key, keys: _*)
    as[Int]
  }

  // SDIFF
  // Return the difference between the Set stored at key1 and all the Sets key2, ..., keyN.
  def sdiff(key: String, keys: String*): Option[Set[Option[String]]] = {
    send("SDIFF", key, keys: _*)
    as[Set[Option[String]]]((x: Iterable[Option[String]]) => Set(x.toSeq: _*))
  }

  // SDIFFSTORE
  // Compute the difference between the Set key1 and all the Sets key2, ..., keyN, 
  // and store the resulting Set at dstkey.
  def sdiffstore(key: String, keys: String*): Option[Int] = {
    send("SDIFFSTORE", key, keys: _*)
    as[Int]
  }

  // SMEMBERS
  // Return all the members of the Set value at key.
  def smembers(key: String): Option[Set[Option[String]]] = {
    send("SMEMBERS", key)
    as[Set[Option[String]]]((x: Iterable[Option[String]]) => Set(x.toSeq: _*))
  }

  // SRANDMEMBER
  // Return a random element from a Set
  def srandmember(key: String): Option[String] = {
    send("SRANDMEMBER", key)
    as[String]
  }
}
