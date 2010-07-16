package com.redis

object RedisClient {
  trait SortOrder
  case object ASC extends SortOrder
  case object DESC extends SortOrder
}

trait Redis extends IO with Protocol {

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

class RedisClient(override val host: String, override val port: Int)
  extends Redis
  with Operations 
  with NodeOperations 
  with StringOperations
  with ListOperations
  with SetOperations
  with SortedSetOperations
  with HashOperations
  with PubSub {

  connect

  def this() = this("localhost", 6379)
  override def toString = host + ":" + String.valueOf(port)
}
