package com.redis.nonblocking

import scala.concurrent.{Promise, ExecutionContext, Await}
import serialization._
import Parse.{Implicits => Parsers}
import RedisCommand._
import RedisReplies._

object ListCommands {
  case class LPush(key: Any, value: Any)(implicit format: Format) extends ListCommand {
    type Ret = Long
    val line = multiBulk("LPUSH".getBytes("UTF-8") +: (Seq(key, value) map format.apply))
    val ret: Array[Byte] => Option[Ret] = RedisReply(_).asLong
  }
  
  case class RPush(key: Any, value: Any)(implicit format: Format) extends ListCommand {
    type Ret = Long
    val line = multiBulk("RPUSH".getBytes("UTF-8") +: (Seq(key, value) map format.apply))
    val ret: Array[Byte] => Option[Ret] = RedisReply(_).asLong
  }
  
  case class LRange[A](key: Any, start: Int, stop: Int)(implicit format: Format, parse: Parse[A]) extends ListCommand {
    type Ret = List[Option[A]]
    val line = multiBulk("LRANGE".getBytes("UTF-8") +: (Seq(key, start, stop) map format.apply))
    val ret: Array[Byte] => Option[Ret] = RedisReply(_).asList
  }
}
