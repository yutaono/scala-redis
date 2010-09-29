package com.redis.ds

trait Deque[A] {
  // inserts at the head
  def addFirst(a: A): Option[Int]

  // inserts at the tail 
  def addLast(a: A): Option[Int]

  // clears the deque
  def clear: Boolean

  // retrieves, but does not remove the head
  def peekFirst: Option[A]

  // retrieves, but does not remove the tail
  def peekLast: Option[A]

  // true, if empty
  def isEmpty: Boolean

  // retrieves and removes the head element of the deque
  def poll: Option[A]

  // retrieves and removes the head element of the deque
  def pollFirst: Option[A]

  // retrieves and removes the tail element of the deque
  def pollLast: Option[A]

  // size of the deque
  def size: Int
}

import com.redis.ListOperations

abstract class RedisDeque(val blocking: Boolean = false, val timeoutInSecs: Int = 0)
  extends Deque[String] { self: ListOperations =>

  val key: String

  def addFirst(a: String) = lpush(key, a) 
  def addLast(a: String) = rpush(key, a)

  def peekFirst = lrange(key, 0, 0) match {
    case Some(l) => Some(l.head.get)
    case None => None
  }

  def peekLast = lrange(key, -1, -1) match {
    case Some(l) => Some(l.head.get)
    case None => None
  }

  def poll =
    if (blocking == true) {
      blpop(timeoutInSecs, key) match {
        case Some(l) => l(1)
        case None => None
      }
    } else lpop(key) 

  def pollFirst = poll

  def pollLast =
    if (blocking == true) {
      brpop(timeoutInSecs, key) match {
        case Some(l) => l(1)
        case None => None
      }
    } else rpop(key) 

  def size = llen(key) match {
    case Some(i) => i
    case _ => 0
  }

  def isEmpty = size == 0

  def clear = size match {
    case 0 => true
    case 1 => 
      val n = poll
      true
    case x => ltrim(key, -1, 0)
  }
}

import com.redis.{Redis, ListOperations}

class RedisDequeClient(val h: String, val p: Int) {
  def getDeque(k: String, blocking: Boolean = false, timeoutInSecs: Int = 0) =
    new RedisDeque(blocking, timeoutInSecs) with ListOperations with Redis {
      val host = h
      val port = p
      val key = k
      connect
    }
}
