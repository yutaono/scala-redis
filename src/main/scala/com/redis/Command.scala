package com.redis

import scala.concurrent.Promise
import ProtocolUtils._

sealed trait RedisCommand {
  // command returns Option[Ret]
  type Ret

  // command input
  val line: Array[Byte]

  // the promise which will be set by the command
  lazy val promise = Promise[Option[Ret]]

  // return value
  val ret: Array[Byte] => Option[Ret]

  // execution of the command
  final def execute(s: Array[Byte]): Promise[Option[Ret]] = promise success ret(s)
}

trait StringCommand extends RedisCommand
trait ListCommand extends RedisCommand
trait KeyCommand extends RedisCommand
trait SetCommand extends RedisCommand

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
