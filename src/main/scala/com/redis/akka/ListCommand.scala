package com.redis.nonblocking

import scala.concurrent.Future
import scala.concurrent.duration._
import serialization._
import Parse.{Implicits => Parsers}
import RedisCommand._
import RedisReplies._
import akka.pattern.ask
import akka.actor._
import akka.util.Timeout

object ListCommands {
  case class LPush(key: Any, value: Any, values: Any*)(implicit format: Format) extends ListCommand {
    type Ret = Long
    val line = multiBulk("LPUSH".getBytes("UTF-8") +: (key :: value :: values.toList) map format.apply)
    val ret: Array[Byte] => Option[Ret] = RedisReply(_).asLong
  }

  case class LPushX(key: Any, value: Any)(implicit format: Format) extends ListCommand {
    type Ret = Long
    val line = multiBulk("LPUSHX".getBytes("UTF-8") +: (Seq(key, value) map format.apply))
    val ret: Array[Byte] => Option[Ret] = RedisReply(_).asLong
  }
  
  case class RPush(key: Any, value: Any, values: Any*)(implicit format: Format) extends ListCommand {
    type Ret = Long
    val line = multiBulk("RPUSH".getBytes("UTF-8") +: (key :: value :: values.toList) map format.apply)
    val ret: Array[Byte] => Option[Ret] = RedisReply(_).asLong
  }

  case class RPushX(key: Any, value: Any)(implicit format: Format) extends ListCommand {
    type Ret = Long
    val line = multiBulk("RPUSHX".getBytes("UTF-8") +: (Seq(key, value) map format.apply))
    val ret: Array[Byte] => Option[Ret] = RedisReply(_).asLong
  }
  
  case class LRange[A](key: Any, start: Int, stop: Int)(implicit format: Format, parse: Parse[A]) extends ListCommand {
    type Ret = List[Option[A]]
    val line = multiBulk("LRANGE".getBytes("UTF-8") +: (Seq(key, start, stop) map format.apply))
    val ret: Array[Byte] => Option[Ret] = RedisReply(_).asList
  }

  case class LLen(key: Any)(implicit format: Format) extends ListCommand {
    type Ret = Long
    val line = multiBulk("LLEN".getBytes("UTF-8") +: (Seq(format.apply(key))))
    val ret: Array[Byte] => Option[Ret] = RedisReply(_).asLong
  }

  case class LTrim(key: Any, start: Int, end: Int)(implicit format: Format) extends ListCommand {
    type Ret = Boolean
    val line = multiBulk("LTRIM".getBytes("UTF-8") +: (Seq(key, start, end) map format.apply))
    val ret: Array[Byte] => Option[Ret] = RedisReply(_).asBoolean
  }
  
  case class LIndex[A](key: Any, index: Int)(implicit format: Format, parse: Parse[A]) extends ListCommand {
    type Ret = A
    val line = multiBulk("LINDEX".getBytes("UTF-8") +: (Seq(key, index) map format.apply))
    val ret: Array[Byte] => Option[Ret] = RedisReply(_).asBulk[A]
  }

  case class LSet(key: Any, index: Int, value: Any)(implicit format: Format) extends ListCommand {
    type Ret = Boolean
    val line = multiBulk("LSET".getBytes("UTF-8") +: (Seq(key, index, value) map format.apply))
    val ret: Array[Byte] => Option[Ret] = RedisReply(_).asBoolean
  }

  case class LRem(key: Any, count: Int, value: Any)(implicit format: Format) extends ListCommand {
    type Ret = Long
    val line = multiBulk("LREM".getBytes("UTF-8") +: (Seq(key, count, value) map format.apply))
    val ret: Array[Byte] => Option[Ret] = RedisReply(_).asLong
  }
  
  case class LPop[A](key: Any)(implicit format: Format, parse: Parse[A]) extends ListCommand {
    type Ret = A
    val line = multiBulk("LPOP".getBytes("UTF-8") +: (Seq(key) map format.apply))
    val ret: Array[Byte] => Option[Ret] = RedisReply(_).asBulk[A]
  }
  
  case class RPop[A](key: Any)(implicit format: Format, parse: Parse[A]) extends ListCommand {
    type Ret = A
    val line = multiBulk("RPOP".getBytes("UTF-8") +: (Seq(key) map format.apply))
    val ret: Array[Byte] => Option[Ret] = RedisReply(_).asBulk[A]
  }
  
  case class RPopLPush[A](srcKey: Any, dstKey: Any)(implicit format: Format, parse: Parse[A]) extends ListCommand {
    type Ret = A
    val line = multiBulk("RPOPLPUSH".getBytes("UTF-8") +: (Seq(srcKey, dstKey) map format.apply))
    val ret: Array[Byte] => Option[Ret] = RedisReply(_).asBulk[A]
  }
  
  case class BRPopLPush[A](srcKey: Any, dstKey: Any, timeoutInSeconds: Int)(implicit format: Format, parse: Parse[A]) extends ListCommand {
    type Ret = A
    val line = multiBulk("BRPOPLPUSH".getBytes("UTF-8") +: (Seq(srcKey, dstKey, timeoutInSeconds) map format.apply))
    val ret: Array[Byte] => Option[Ret] = RedisReply(_).asBulk[A]
  }
  
  case class BLPop[K, V](timeoutInSeconds: Int, key: K, keys: K*)
    (implicit format: Format, parseK: Parse[K], parseV: Parse[V]) extends ListCommand {
    type Ret = (K, V)
    val line = multiBulk("BLPOP".getBytes("UTF-8") +: ((key :: keys.foldRight(List[Any](timeoutInSeconds))(_ :: _)) map format.apply))
    val ret: Array[Byte] => Option[Ret] = RedisReply(_).asListPairs[K,V].flatMap(_.flatten.headOption)
  }
  
  case class BRPop[K, V](timeoutInSeconds: Int, key: K, keys: K*)
    (implicit format: Format, parseK: Parse[K], parseV: Parse[V]) extends ListCommand {
    type Ret = (K, V)
    val line = multiBulk("BRPOP".getBytes("UTF-8") +: ((key :: keys.foldRight(List[Any](timeoutInSeconds))(_ :: _)) map format.apply))
    val ret: Array[Byte] => Option[Ret] = RedisReply(_).asListPairs[K,V].flatMap(_.flatten.headOption)
  }
}

object ListOperations {
  import ListCommands._

  implicit val timeout = Timeout(5 seconds)

  // LPUSH (Variadic: >= 2.4)
  // add values to the head of the list stored at key
  def lpush(key: Any, value: Any, values: Any*)(implicit format: Format): ActorRef => Future[Option[Long]] = {client: ActorRef =>
    client.ask(LPush(key, value, values)).mapTo[Option[Long]] 
  }

  // LPUSHX (Variadic: >= 2.4)
  // add value to the tail of the list stored at key
  def lpushx(key: Any, value: Any)(implicit format: Format): ActorRef => Future[Option[Long]] = {client: ActorRef =>
    client.ask(LPushX(key, value)).mapTo[Option[Long]] 
  }

  // RPUSH (Variadic: >= 2.4)
  // add values to the head of the list stored at key
  def rpush(key: Any, value: Any, values: Any*)(implicit format: Format): ActorRef => Future[Option[Long]] = {client: ActorRef =>
    client.ask(RPush(key, value, values)).mapTo[Option[Long]] 
  }

  // RPUSHX (Variadic: >= 2.4)
  // add value to the tail of the list stored at key
  def rpushx(key: Any, value: Any)(implicit format: Format): ActorRef => Future[Option[Long]] = {client: ActorRef =>
    client.ask(RPushX(key, value)).mapTo[Option[Long]] 
  }

  // LLEN
  // return the length of the list stored at the specified key.
  // If the key does not exist zero is returned (the same behaviour as for empty lists). 
  // If the value stored at key is not a list an error is returned.
  def llen(key: Any)(implicit format: Format): ActorRef => Future[Option[Long]] = {client: ActorRef =>
    client.ask(LLen(key)).mapTo[Option[Long]] 
  }

  // LRANGE
  // return the specified elements of the list stored at the specified key.
  // Start and end are zero-based indexes. 
  def lrange[A](key: Any, start: Int, end: Int)(implicit format: Format, parse: Parse[A]): ActorRef => Future[Option[List[Option[A]]]] = {client: ActorRef =>
    client.ask(LRange(key, start, end)).mapTo[Option[List[Option[A]]]] 
  }

  // LTRIM
  // Trim an existing list so that it will contain only the specified range of elements specified.
  def ltrim(key: Any, start: Int, end: Int)(implicit format: Format): ActorRef => Future[Option[Boolean]] = {client: ActorRef =>
    client.ask(LTrim(key, start, end)).mapTo[Option[Boolean]] 
  }

  // LINDEX
  // return the especified element of the list stored at the specified key. 
  // Negative indexes are supported, for example -1 is the last element, -2 the penultimate and so on.
  def lindex[A](key: Any, index: Int)(implicit format: Format, parse: Parse[A]): ActorRef => Future[Option[A]] = {client: ActorRef =>
    client.ask(LIndex(key, index)).mapTo[Option[A]] 
  }

  // LSET
  // set the list element at index with the new value. Out of range indexes will generate an error
  def lset(key: Any, index: Int, value: Any)(implicit format: Format): ActorRef => Future[Option[Boolean]] = {client: ActorRef =>
    client.ask(LSet(key, index, value)).mapTo[Option[Boolean]] 
  }

  // LREM
  // Remove the first count occurrences of the value element from the list.
  def lrem(key: Any, count: Int, value: Any)(implicit format: Format): ActorRef => Future[Option[Long]] = {client: ActorRef =>
    client.ask(LRem(key, count, value)).mapTo[Option[Long]] 
  }

  // LPOP
  // atomically return and remove the first (LPOP) or last (RPOP) element of the list
  def lpop[A](key: Any)(implicit format: Format, parse: Parse[A]): ActorRef => Future[Option[A]] = {client: ActorRef =>
    client.ask(LPop(key)).mapTo[Option[A]] 
  }

  // RPOP
  // atomically return and remove the first (LPOP) or last (RPOP) element of the list
  def rpop[A](key: Any)(implicit format: Format, parse: Parse[A]): ActorRef => Future[Option[A]] = {client: ActorRef =>
    client.ask(RPop(key)).mapTo[Option[A]] 
  }

  // RPOPLPUSH
  // Remove the first count occurrences of the value element from the list.
  def rpoplpush[A](srcKey: Any, dstKey: Any)
    (implicit format: Format, parse: Parse[A]): ActorRef => Future[Option[A]] = {client: ActorRef =>
    client.ask(RPopLPush(srcKey, dstKey)).mapTo[Option[A]] 
  }

  def brpoplpush[A](srcKey: Any, dstKey: Any, timeoutInSeconds: Int)
    (implicit format: Format, parse: Parse[A]): ActorRef => Future[Option[A]] = {client: ActorRef =>
    client.ask(BRPopLPush(srcKey, dstKey, timeoutInSeconds)).mapTo[Option[A]] 
  }

  def blpop[K,V](timeoutInSeconds: Int, key: K, keys: K*)
    (implicit format: Format, parseK: Parse[K], parseV: Parse[V]): ActorRef => Future[Option[(K,V)]] = {client: ActorRef =>
    client.ask(BLPop[K, V](timeoutInSeconds, key, keys:_*)).mapTo[Option[(K, V)]] 
  }

  def brpop[K,V](timeoutInSeconds: Int, key: K, keys: K*)
    (implicit format: Format, parseK: Parse[K], parseV: Parse[V]): ActorRef => Future[Option[(K,V)]] = {client: ActorRef =>
    client.ask(BRPop[K, V](timeoutInSeconds, key, keys:_*)).mapTo[Option[(K, V)]] 
  }
}
