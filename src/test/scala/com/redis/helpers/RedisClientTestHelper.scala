package com.redis

import org.specs._
import com.redis._
import com.redis.operations._

class RedisTestClient(val connection: Connection) extends Operations 
  with ListOperations with SetOperations with SortedSetOperations with NodeOperations with KeySpaceOperations with SortOperations {
  var db: Int = 0
  def getConnection(key: String): Connection = connection
  println("connection = " + connection)
}