package com.redis.nonblocking

import scala.concurrent.Promise
import ProtocolUtils._

sealed trait RedisCommand {
  def line: Array[Byte]
  def promise: Promise[_]
  def reply(s: Array[Byte]): Promise[_ <: Option[_]]
}

trait StringCommand extends RedisCommand
trait ListCommand extends RedisCommand

object RedisCommand {
  def multiBulk(args: Seq[Array[Byte]]): Array[Byte] = {
    val b = new scala.collection.mutable.ArrayBuilder.ofByte
    b ++= "*%d".format(args.size).getBytes
    b ++= LS
    args foreach { arg =>
      b ++= "$%d".format(arg.size).getBytes
      b ++= LS
      b ++= arg
      b ++= LS
    }
    b.result
  }
}
