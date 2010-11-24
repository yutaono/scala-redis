package com.redis

import org.apache.commons.pool._
import org.apache.commons.pool.impl._


private [redis] class RedisClientFactory(host: String, port: Int) extends PoolableObjectFactory {
  def makeObject = new RedisClient(host, port)
  def destroyObject(rc: Object): Unit = rc.asInstanceOf[RedisClient].disconnect
  def passivateObject(rc: Object): Unit = rc.asInstanceOf[RedisClient].disconnect
  def validateObject(rc: Object) = rc.asInstanceOf[RedisClient].connected == true
  def activateObject(rc: Object): Unit = rc.asInstanceOf[RedisClient].connect
}

class RedisClientPool(host: String, port: Int) {
  val pool = new StackObjectPool(new RedisClientFactory(host, port))

  def withClient[T](body: RedisClient => T) = {
    val client = pool.borrowObject.asInstanceOf[RedisClient]
    try {
      body(client)
    } finally {
      pool.returnObject(client)
    }
  }
}
