package com.redis

trait Operations { self: RedisClient =>
  import self._

  // KEYS
  // returns all the keys matching the glob-style pattern.
  def keys(pattern: String): Option[Array[String]] = {
    send("KEYS", pattern)
    asString match {
      case Some(s) => Some(s split " ")
      case _ => None
    }
  }

  // RANDKEY
  // return a randomly selected key from the currently selected DB.
  def randomKey: Option[String] = {
    send("RANDOMKEY")
    asString
  }
  
  // RENAME (oldkey, newkey)
  // atomically renames the key oldkey to newkey.
  def rename(oldkey: String, newkey: String): Boolean = {
    send("RENAME", oldkey, newkey)
    asBoolean
  }
  
  // RENAMENX (oldkey, newkey)
  // rename oldkey into newkey but fails if the destination key newkey already exists.
  def renamenx(oldkey: String, newkey: String): Boolean = {
    send("RENAMENX", oldkey, newkey)
    asBoolean
  }
  
  // DBSIZE
  // return the size of the db.
  def dbSize: Option[Int] = {
    send("DBSIZE")
    asInt
  }

  // EXISTS (key)
  // test if the specified key exists.
  def exists(key: String): Boolean = {
    send("EXISTS", key)
    asBoolean
  }

  // DELETE (key1 key2 ..)
  // deletes the specified keys.
  def delete(key: String, keys: String*): Option[Int] = {
    send("DEL", key, keys: _*)
    asInt
  }

  // TYPE (key)
  // return the type of the value stored at key in form of a string.
  def getType(key: String): Option[String] = {
    send("TYPE", key)
    asString
  }

  // EXPIRE (key, expiry)
  // sets the expire time (in sec.) for the specified key.
  def expire(key: String, expiry: Int): Boolean = {
    send("EXPIRE", key, String.valueOf(expiry))
    asBoolean
  }

  // SELECT (index)
  // selects the DB to connect, defaults to 0 (zero).
  def selectDb(index: Int): Boolean = {
    send("SELECT", String.valueOf(index))
    asBoolean match {
      case true => {
        db = index
        true
      }
      case _ => false
    }
  }
  
  // FLUSHDB the DB
  // removes all the DB data.
  def flushDb: Boolean = {
    send("FLUSHDB")
    asBoolean
  }
  
  // FLUSHALL the DB's
  // removes data from all the DB's.
  def flushAll: Boolean = {
    send("FLUSHALL")
    asBoolean
  }

  // MOVE
  // Move the specified key from the currently selected DB to the specified destination DB.
  def move(key: String, db: Int) = {
    send("MOVE", key, String.valueOf(db))
    asBoolean
  }
  
  // QUIT
  // exits the server.
  def quit: Boolean = {
    send("QUIT")
    disconnect
  }
  
  // AUTH
  // auths with the server.
  def auth(secret: String): Boolean = {
    send("AUTH", secret)
    asBoolean
  }
}
