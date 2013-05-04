package com.redis.nonblocking

import scala.concurrent.{Promise, ExecutionContext, Await}
import serialization._
import Parse.{Implicits => Parsers}
import RedisCommand._
import RedisReplies._

object StringCommands {
  case class Get[A](key: Any)(implicit format: Format, parse: Parse[A]) extends StringCommand {
    val line = multiBulk("GET".getBytes("UTF-8") +: Seq(format.apply(key)))
    val promise = Promise[Option[A]]
    def reply(s: Array[Byte]) = {
      promise success RedisReply(s).asBulk[A]
    }
  }
  
  case class Set(key: Any, value: Any)(implicit format: Format) extends StringCommand {
    val line = multiBulk("SET".getBytes("UTF-8") +: (Seq(key, value) map format.apply))
    val promise = Promise[Option[Boolean]]
    def reply(s: Array[Byte]) = {
      promise success RedisReply(s).asBoolean
    }
  }
}
