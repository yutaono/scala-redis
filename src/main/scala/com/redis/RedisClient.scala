package com.redis

object RedisClient {
  trait SortOrder
  case object ASC extends SortOrder
  case object DESC extends SortOrder
}

class RedisClient(val host: String, val port: Int)
  extends IO 
  with Protocol
  with Operations 
  with StringOperations
  with ListOperations
  with SetOperations
  with SortedSetOperations {

  connect

  def this() = this("localhost", 6379)

  def send(command: String, key: String, values: String*) = {
    snd(command, key, values:_*) { write }
  }

  def send(command: String) = {
    snd(command) { write }
  }

  def cmd(command: String, key: String, values: String*) = {
    MultiBulkCommand(command, key, values:_*).toString
  }

  def cmd(command: String) = {
    InlineCommand(command).toString
  }
}

