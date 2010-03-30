package com.redis

trait Operations { self: Redis =>

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
  def randkey: Option[String] = {
    send("RANDOMKEY")
    asString
  }

  @deprecated("use randkey") def randomKey = randkey
  
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
  def dbsize: Option[Int] = {
    send("DBSIZE")
    asInt
  }

  @deprecated("use dbsize") def dbSize = dbsize 

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
  def select(index: Int): Boolean = {
    send("SELECT", String.valueOf(index))
    asBoolean match {
      case true => {
        db = index
        true
      }
      case _ => false
    }
  }

  @deprecated("use selectdb") def selectDb(index: Int) = select(index)
  
  // FLUSHDB the DB
  // removes all the DB data.
  def flushdb: Boolean = {
    send("FLUSHDB")
    asBoolean
  }
  
  @deprecated("use flushdb") def flushDb = flushdb

  // FLUSHALL the DB's
  // removes data from all the DB's.
  def flushall: Boolean = {
    send("FLUSHALL")
    asBoolean
  }

  def flushAll = flushall

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
