package com.redis

import serialization.Format

object RedisClient {
  trait SortOrder
  case object ASC extends SortOrder
  case object DESC extends SortOrder
}

trait Redis extends IO with Protocol {
  def send(command: String, args: Seq[Any])(implicit format: Format): Unit = write(Commands.multiBulk(command.getBytes("UTF-8") +: (args map (format.apply))))
  def send(command: String): Unit = write(Commands.multiBulk(List(command.getBytes("UTF-8"))))
  def cmd(args: Seq[Array[Byte]]) = Commands.multiBulk(args)

  protected def flattenPairs(in: Iterable[(Any, Any)]): List[Any] =
    in.iterator.flatMap(x => Iterator(x._1, x._2)).toList
}

trait RedisCommand extends Redis
  with Operations 
  with NodeOperations 
  with StringOperations
  with ListOperations
  with SetOperations
  //with SortedSetOperations
  with HashOperations
  

class RedisClient(override val host: String, override val port: Int)
  extends RedisCommand 
  /*with PubSub*/ {

  connect

  def this() = this("localhost", 6379)
  override def toString = host + ":" + String.valueOf(port)

  def pipeline(f: => Unit): Option[List[Option[_]]] = {
    send("MULTI")
    val ok = asString // flush reply stream
    try {
      f
      send("EXEC")
      asExec
    } catch {
      case e: RedisMultiExecException => 
        send("DISCARD")
        val ok = asString
        None
    }
  }
}
