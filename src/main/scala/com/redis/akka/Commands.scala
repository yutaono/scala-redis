package com.redis.akka

import scala.concurrent.{Promise, ExecutionContext, Await}
import serialization._
import Parse.{Implicits => Parsers}

object RedisCommands {
  import RedisReplies._

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

  sealed trait RedisCommand[R] {
    def line: Array[Byte]
    def promise: Promise[_]
    def reply(s: Array[Byte]): Option[R]
  }

  case class Get[A](key: Any)(implicit format: Format, parse: Parse[A]) extends RedisCommand[A] {
    val line = multiBulk("GET".getBytes("UTF-8") +: Seq(format.apply(key)))
    val promise = Promise[Option[A]]
    def reply(s: Array[Byte]) = {
      val r = RedisReply(s).asBulk[A]
      promise.success(r)
      r
    }
  }
  case class Set(key: Any, value: Any)(implicit format: Format) extends RedisCommand[Boolean] {
    val line = multiBulk("SET".getBytes("UTF-8") +: (Seq(key, value) map format.apply))
    val promise = Promise[Option[Boolean]]
    def reply(s: Array[Byte]) = {
      val r = RedisReply(s).asBoolean
      promise.success(r)
      r
    }
  }
  case class LPush(key: Any, value: Any)(implicit format: Format) extends RedisCommand[Long] {
    val line = multiBulk("LPUSH".getBytes("UTF-8") +: (Seq(key, value) map format.apply))
    val promise = Promise[Option[Long]]
    def reply(s: Array[Byte]) = {
      val r = RedisReply(s).asLong
      promise.success(r)
      r
    }
  }
  case class RPush(key: Any, value: Any)(implicit format: Format) extends RedisCommand[Long] {
    val line = multiBulk("RPUSH".getBytes("UTF-8") +: (Seq(key, value) map format.apply))
    val promise = Promise[Option[Long]]
    def reply(s: Array[Byte]) = {
      val r = RedisReply(s).asLong
      promise.success(r)
      r
    }
  }
  case class LRange[A](key: Any, start: Int, stop: Int)(implicit format: Format, parse: Parse[A]) 
    extends RedisCommand[List[Option[A]]] {
    val line = multiBulk("LRANGE".getBytes("UTF-8") +: (Seq(key, start, stop) map format.apply))
    val promise = Promise[Option[List[Option[A]]]]
    def reply(s: Array[Byte]) = {
      val r = RedisReply(s).asList
      promise.success(r)
      r
    }
  }
}
