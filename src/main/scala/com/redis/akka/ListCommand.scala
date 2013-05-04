package com.redis.nonblocking

import scala.concurrent.{Promise, ExecutionContext, Await}
import serialization._
import Parse.{Implicits => Parsers}
import RedisCommand._
import RedisReplies._

object ListCommands {
  case class LPush(key: Any, value: Any)(implicit format: Format) extends ListCommand {
    val line = multiBulk("LPUSH".getBytes("UTF-8") +: (Seq(key, value) map format.apply))
    val promise = Promise[Option[Long]]
    def reply(s: Array[Byte]) = {
      promise success RedisReply(s).asLong
    }
  }
  
  case class RPush(key: Any, value: Any)(implicit format: Format) extends ListCommand {
    val line = multiBulk("RPUSH".getBytes("UTF-8") +: (Seq(key, value) map format.apply))
    val promise = Promise[Option[Long]]
    def reply(s: Array[Byte]) = {
      promise success RedisReply(s).asLong
    }
  }
  
  case class LRange[A](key: Any, start: Int, stop: Int)(implicit format: Format, parse: Parse[A]) extends ListCommand {
    val line = multiBulk("LRANGE".getBytes("UTF-8") +: (Seq(key, start, stop) map format.apply))
    val promise = Promise[Option[List[Option[A]]]]
    def reply(s: Array[Byte]) = {
      promise success RedisReply(s).asList
    }
  }
}
