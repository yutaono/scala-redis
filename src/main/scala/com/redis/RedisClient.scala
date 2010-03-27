package com.redis

object RedisClient {
  trait SortOrder
  case object ASC extends SortOrder
  case object DESC extends SortOrder
}

// class Redis(val host: String, val port: Int) extends IO with Protocol {
trait Redis extends IO with Protocol {

  val host: String
  val port: Int

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
  with StringOperations
  with ListOperations
  with SetOperations
  with SortedSetOperations {

  connect

  def this() = this("localhost", 6379)
}
