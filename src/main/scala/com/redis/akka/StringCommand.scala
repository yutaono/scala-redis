package com.redis.nonblocking

import scala.concurrent.Future
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

  case class GetSet[A](key: Any, value: Any)(implicit format: Format, parse: Parse[A]) extends StringCommand {
    type Ret = A
    val line = multiBulk("GETSET".getBytes("UTF-8") +: (Seq(key, value) map format.apply))
    val ret: Array[Byte] => Option[Ret] = RedisReply(_).asBulk[A]
  }
  
  case class SetNx(key: Any, value: Any)(implicit format: Format) extends StringCommand {
    type Ret = Boolean
    val line = multiBulk("SETNX".getBytes("UTF-8") +: (Seq(key, value) map format.apply))
    val ret: Array[Byte] => Option[Ret] = RedisReply(_).asBoolean
  }
  
  case class SetEx(key: Any, expiry: Int, value: Any)(implicit format: Format) extends StringCommand {
    type Ret = Boolean
    val line = multiBulk("SETEX".getBytes("UTF-8") +: (Seq(key, expiry, value) map format.apply))
    val ret: Array[Byte] => Option[Ret] = RedisReply(_).asBoolean
  }
  
  case class Incr(key: Any, by: Option[Int] = None)(implicit format: Format) extends StringCommand {
    type Ret = Long
    val line = multiBulk(
      by.map(i => "INCRBY".getBytes("UTF-8") +: (Seq(key, i) map format.apply))
        .getOrElse("INCR".getBytes("UTF-8") +: (Seq(format.apply(key))))
    )
    val ret: Array[Byte] => Option[Ret] = RedisReply(_).asLong
  }
  
  case class Decr(key: Any, by: Option[Int] = None)(implicit format: Format) extends StringCommand {
    type Ret = Long
    val line = multiBulk(
      by.map(i => "DECRBY".getBytes("UTF-8") +: (Seq(key, i) map format.apply))
        .getOrElse("DECR".getBytes("UTF-8") +: (Seq(format.apply(key))))
    )
    val ret: Array[Byte] => Option[Ret] = RedisReply(_).asLong
  }

  case class MGet[A](key: Any, keys: Any*)(implicit format: Format, parse: Parse[A]) extends StringCommand {
    type Ret = List[Option[A]]
    val line = multiBulk("MGET".getBytes("UTF-8") +: ((key :: keys.toList) map format.apply))
    val ret: Array[Byte] => Option[Ret] = RedisReply(_).asList[A]
  }

  case class MSet(kvs: (Any, Any)*)(implicit format: Format) extends StringCommand {
    type Ret = Boolean
    val line = multiBulk("MSET".getBytes("UTF-8") +: (kvs.foldRight(List[Any]()){ case ((k,v),l) => k :: v :: l }) map format.apply)
    val ret: Array[Byte] => Option[Ret] = RedisReply(_).asBoolean
  }

  case class MSetNx(kvs: (Any, Any)*)(implicit format: Format) extends StringCommand {
    type Ret = Boolean
    val line = multiBulk("MSETNX".getBytes("UTF-8") +: (kvs.foldRight(List[Any]()){ case ((k,v),l) => k :: v :: l }) map format.apply)
    val ret: Array[Byte] => Option[Ret] = RedisReply(_).asBoolean
  }
  
  case class SetRange(key: Any, offset: Int, value: Any)(implicit format: Format) extends StringCommand {
    type Ret = Long
    val line = multiBulk("SETRANGE".getBytes("UTF-8") +: (Seq(key, offset, value) map format.apply))
    val ret: Array[Byte] => Option[Ret] = RedisReply(_).asLong
  }

  case class GetRange[A](key: Any, start: Int, end: Int)(implicit format: Format, parse: Parse[A]) extends StringCommand {
    type Ret = A
    val line = multiBulk("GETRANGE".getBytes("UTF-8") +: (Seq(key, start, end) map format.apply))
    val ret: Array[Byte] => Option[Ret] = RedisReply(_).asBulk[A]
  }
  
  case class Strlen(key: Any)(implicit format: Format) extends StringCommand {
    type Ret = Long
    val line = multiBulk("STRLEN".getBytes("UTF-8") +: Seq(format.apply(key)))
    val ret: Array[Byte] => Option[Ret] = RedisReply(_).asLong
  }
  
  case class Append(key: Any, value: Any)(implicit format: Format) extends StringCommand {
    type Ret = Long
    val line = multiBulk("APPEND".getBytes("UTF-8") +: (Seq(key, value) map format.apply))
    val ret: Array[Byte] => Option[Ret] = RedisReply(_).asLong
  }
  
  case class GetBit(key: Any, offset: Int)(implicit format: Format) extends StringCommand {
    type Ret = Long
    val line = multiBulk("GETBIT".getBytes("UTF-8") +: (Seq(key, offset) map format.apply))
    val ret: Array[Byte] => Option[Ret] = RedisReply(_).asLong
  }
  
  case class SetBit(key: Any, offset: Int, value: Any)(implicit format: Format) extends StringCommand {
    type Ret = Long
    val line = multiBulk("SETBIT".getBytes("UTF-8") +: (Seq(key, offset, value) map format.apply))
    val ret: Array[Byte] => Option[Ret] = RedisReply(_).asLong
  }
  
  case class BitOp(op: String, destKey: Any, srcKeys: Any*)(implicit format: Format) extends StringCommand {
    type Ret = Long
    val line = multiBulk("BITOP".getBytes("UTF-8") +: ((op :: destKey :: srcKeys.toList) map format.apply))
    val ret: Array[Byte] => Option[Ret] = RedisReply(_).asLong
  }
  
  case class BitCount(key: Any, range: Option[(Int, Int)])(implicit format: Format) extends StringCommand {
    type Ret = Long
    val line = multiBulk("BITCOUNT".getBytes("UTF-8") +: (List(key) ++ (range.map(_.productIterator.toList).getOrElse(List()))) map format.apply)
    val ret: Array[Byte] => Option[Ret] = RedisReply(_).asLong
  }
}

object StringOperations {

  import StringCommands._

  implicit val timeout = Timeout(5 seconds)

  // GET (key)
  // gets the value for the specified key.
  def get[A](key: Any)(implicit format: Format, parse: Parse[A]): ActorRef => Future[Option[A]] = {client: ActorRef =>
    client.ask(Get[A](key)).mapTo[Option[A]] 
  }

  // SET KEY (key, value)
  // sets the key with the specified value.
  def set(key: Any, value: Any)(implicit format: Format): ActorRef => Future[Option[Boolean]] = {client: ActorRef =>
    client.ask(Set(key, value)).mapTo[Option[Boolean]] 
  }
  
  // GETSET (key, value)
  // is an atomic set this value and return the old value command.
  def getset[A](key: Any, value: Any)
    (implicit format: Format, parse: Parse[A]): ActorRef => Future[Option[A]] = {client: ActorRef =>
    client.ask(GetSet[A](key, value)).mapTo[Option[A]] 
  }

  // SETNX (key, value)
  // sets the value for the specified key, only if the key is not there.
  def setnx(key: Any, value: Any)(implicit format: Format): ActorRef => Future[Option[Boolean]] = {client: ActorRef =>
    client.ask(SetNx(key, value)).mapTo[Option[Boolean]] 
  }

  // SETEX (key, expiry, value)
  // sets the value for the specified key, with an expiry
  def setex(key: Any, expiry: Int, value: Any)
    (implicit format: Format): ActorRef => Future[Option[Boolean]] = {client: ActorRef =>
    client.ask(SetEx(key, expiry, value)).mapTo[Option[Boolean]] 
  }

  // INCR (key)
  // increments the specified key by 1
  def incr(key: Any)(implicit format: Format): ActorRef => Future[Option[Long]] = {client: ActorRef =>
    client.ask(Incr(key)).mapTo[Option[Long]] 
  }

  // INCRBY (key, by)
  // increments the specified key by increment
  def incrby(key: Any, by: Int)(implicit format: Format): ActorRef => Future[Option[Long]] = {client: ActorRef =>
    client.ask(Incr(key, Some(by))).mapTo[Option[Long]] 
  }

  // DECR (key)
  // decrements the specified key by 1
  def decr(key: Any)(implicit format: Format): ActorRef => Future[Option[Long]] = {client: ActorRef =>
    client.ask(Decr(key)).mapTo[Option[Long]] 
  }

  // DECR (key, by)
  // decrements the specified key by increment
  def decrby(key: Any, by: Int)(implicit format: Format): ActorRef => Future[Option[Long]] = {client: ActorRef =>
    client.ask(Decr(key, Some(by))).mapTo[Option[Long]] 
  }

  // MGET (key, key, key, ...)
  // get the values of all the specified keys.
  def mget[A](key: Any, keys: Any*)
    (implicit format: Format, parse: Parse[A]): ActorRef => Future[Option[List[Option[A]]]] = {client: ActorRef =>
    client.ask(MGet[A](key, keys)).mapTo[Option[List[Option[A]]]] 
  }

  // MSET (key1 value1 key2 value2 ..)
  // set the respective key value pairs. Overwrite value if key exists
  def mset(kvs: (Any, Any)*)(implicit format: Format): ActorRef => Future[Option[Boolean]] = {client: ActorRef =>
    client.ask(MSet(kvs:_*)).mapTo[Option[Boolean]]
  }

  // MSETNX (key1 value1 key2 value2 ..)
  // set the respective key value pairs. Noop if any key exists
  def msetnx(kvs: (Any, Any)*)(implicit format: Format): ActorRef => Future[Option[Boolean]] = {client: ActorRef =>
    client.ask(MSetNx(kvs:_*)).mapTo[Option[Boolean]]
  }

  // SETRANGE key offset value
  // Overwrites part of the string stored at key, starting at the specified offset, 
  // for the entire length of value.
  def set(key: Any, offset: Int, value: Any)
    (implicit format: Format): ActorRef => Future[Option[Long]] = {client: ActorRef =>
    client.ask(SetRange(key, offset, value)).mapTo[Option[Long]] 
  }

  // GETRANGE key start end
  // Returns the substring of the string value stored at key, determined by the offsets 
  // start and end (both are inclusive).
  def getrange[A](key: Any, start: Int, end: Int)
    (implicit format: Format, parse: Parse[A]): ActorRef => Future[Option[A]] = {client: ActorRef =>
    client.ask(GetRange[A](key, start, end)).mapTo[Option[A]] 
  }

  // STRLEN key
  // gets the length of the value associated with the key
  def strlen(key: Any)(implicit format: Format): ActorRef => Future[Option[Long]] = {client: ActorRef =>
    client.ask(Strlen(key)).mapTo[Option[Long]] 
  }

  // APPEND KEY (key, value)
  // appends the key value with the specified value.
  def append(key: Any, value: Any)(implicit format: Format): ActorRef => Future[Option[Long]] = {client: ActorRef =>
    client.ask(Append(key, value)).mapTo[Option[Long]] 
  }

  // GETBIT key offset
  // Returns the bit value at offset in the string value stored at key
  def getbit(key: Any, offset: Int)(implicit format: Format): ActorRef => Future[Option[Long]] = {client: ActorRef =>
    client.ask(GetBit(key, offset)).mapTo[Option[Long]] 
  }

  // SETBIT key offset value
  // Sets or clears the bit at offset in the string value stored at key
  def setbit(key: Any, offset: Int, value: Any)
    (implicit format: Format): ActorRef => Future[Option[Long]] = {client: ActorRef =>
    client.ask(SetBit(key, offset, value)).mapTo[Option[Long]] 
  }

  // BITOP op destKey srcKey...
  // Perform a bitwise operation between multiple keys (containing string values) and store the result in the destination key.
  def bitop(op: String, destKey: Any, srcKeys: Any*)
    (implicit format: Format): ActorRef => Future[Option[Long]] = {client: ActorRef =>
    client.ask(BitOp(op, destKey, srcKeys)).mapTo[Option[Long]] 
  }

  // BITCOUNT key range
  // Count the number of set bits in the given key within the optional range
  def bitcount(key: Any, range: Option[(Int, Int)] = None)
    (implicit format: Format): ActorRef => Future[Option[Long]] = {client: ActorRef =>
    client.ask(BitCount(key, range)).mapTo[Option[Long]] 
  }
}
