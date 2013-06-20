package com.redis.nonblocking

import scala.concurrent.duration._
import serialization._
import Parse.{Implicits => Parsers}
import akka.pattern.ask
import akka.actor._
import akka.util.Timeout
import RedisCommand._
import RedisReplies._

object StringCommands {
  case class Get[A](key: Any)(implicit format: Format, parse: Parse[A]) extends StringCommand {
    type Ret = A
    val line = multiBulk("GET".getBytes("UTF-8") +: Seq(format.apply(key)))
    val ret: Array[Byte] => Option[Ret] = RedisReply(_).asBulk[A]
  }
  
  case class Set(key: Any, value: Any)(implicit format: Format) extends StringCommand {
    type Ret = Boolean
    val line = multiBulk("SET".getBytes("UTF-8") +: (Seq(key, value) map format.apply))
    val ret: Array[Byte] => Option[Ret] = RedisReply(_).asBoolean
  }

  implicit val timeout = Timeout(5 seconds)

  def get[A](key: Any)(implicit format: Format, parse: Parse[A]) = {client: ActorRef =>
    client.ask(Get[String](key)).mapTo[Option[String]] 
  }

  def set(key: Any, value: Any)(implicit format: Format) = {client: ActorRef =>
    client.ask(Set(key, value)).mapTo[Option[Boolean]] 
  }
}
