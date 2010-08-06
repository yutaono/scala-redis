package com.redis

trait ListOperations { self: Redis =>

  // LPUSH
  // add string value to the head of the list stored at key
  def lpush(key: String, value: String): Option[Int] = {
    send("LPUSH", key, value)
    as[Int]
  }

  // RPUSH
  // add string value to the head of the list stored at key
  def rpush(key: String, value: String): Option[Int] = {
    send("RPUSH", key, value)
    as[Int]
  }

  // LLEN
  // return the length of the list stored at the specified key.
  // If the key does not exist zero is returned (the same behaviour as for empty lists). 
  // If the value stored at key is not a list an error is returned.
  def llen(key: String): Option[Int] = {
    send("LLEN", key)
    as[Int]
  }

  // LRANGE
  // return the specified elements of the list stored at the specified key.
  // Start and end are zero-based indexes. 
  def lrange(key: String, start: Int, end: Int): Option[List[Option[String]]] = {
    send("LRANGE", key, String.valueOf(start), String.valueOf(end))
    as[List[Option[String]]]
  }

  // LTRIM
  // Trim an existing list so that it will contain only the specified range of elements specified.
  def ltrim(key: String, start: Int, end: Int): Boolean = {
    send("LTRIM", key, String.valueOf(start), String.valueOf(end))
    asBoolean
  }

  // LINDEX
  // return the especified element of the list stored at the specified key. 
  // Negative indexes are supported, for example -1 is the last element, -2 the penultimate and so on.
  def lindex(key: String, index: Int): Option[String] = {
    send("LINDEX", key, String.valueOf(index))
    as[String]
  }

  // LSET
  // set the list element at index with the new value. Out of range indexes will generate an error
  def lset(key: String, index: Int, value: String): Boolean = {
    send("LSET", key, String.valueOf(index), value)
    asBoolean
  }

  // LREM
  // Remove the first count occurrences of the value element from the list.
  def lrem(key: String, count: Int, value: String): Option[Int] = {
    send("LREM", key, String.valueOf(count), value)
    as[Int]
  }

  // LPOP
  // atomically return and remove the first (LPOP) or last (RPOP) element of the list
  def lpop(key: String): Option[String] = {
    send("LPOP", key)
    as[String]
  }

  // RPOP
  // atomically return and remove the first (LPOP) or last (RPOP) element of the list
  def rpop(key: String): Option[String] = {
    send("RPOP", key)
    as[String]
  }

  // RPOPLPUSH
  // Remove the first count occurrences of the value element from the list.
  def rpoplpush(srcKey: String, dstKey: String): Option[String] = {
    send("RPOPLPUSH", srcKey, dstKey)
    as[String]
  }

  def blpop(timeoutInSeconds: Int, key: String, keys: String*): Option[List[Option[String]]] = {
    send("BLPOP", key, (keys.toList ::: List(String.valueOf(timeoutInSeconds))): _*)
    as[List[Option[String]]]
  }
}
