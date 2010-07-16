package com.redis

trait HashOperations { self: Redis =>
  def hset(key : String, field : String, value : String): Boolean = {
    send("HSET", key, field, value)
    asBoolean
  }
  
  def hget(key : String, field : String) : Option[String] = {
    send("HGET", key, field)
    asString
  }
  
  def hmset(key : String, map : Map[String,String]) : Boolean = {
    send("HMSET", key, map.flatMap { case (field, value) =>
      List(field, value)
    }.toSeq : _*)
    asBoolean
  }
  
  def hmget(key : String, fields : String*) : Option[Map[String,String]] = {
    send("HMGET", key, fields : _*)
    asList.map { values =>
      fields.zip(values).flatMap {
        case (field,Some(value)) =>
          List((field,value))
        case (field,None) =>
          Nil
      }.toMap
    }
  }
  
  def hincrby(key : String, field : String, value : Int) : Option[Int] = {
    send("HINCRBY", key, field, value.toString)
    asInt
  }
  
  def hexists(key : String, field : String) : Boolean = {
    send("HEXISTS", key, field)
    asBoolean
  }
  
  def hdel(key : String, field : String) : Boolean = {
    send("HDEL", key, field)
    asBoolean
  }
  
  def hlen(key : String) : Option[Int] = {
    send("HLEN", key)
    asInt
  }
  
  def hkeys(key : String) : Option[List[String]] = {
    send("HKEYS", key)
    asList.map(_.flatMap { 
      case Some(v) => List(v)
      case None => Nil
    })
  }
  
  def hvals(key : String) : Option[List[String]] = {
    send("HVALS", key)
    asList.map(_.flatMap {
      case Some(v) => List(v)
      case None => Nil
    })
  }
  
  def hgetall(key : String) : Option[Map[String,String]] = {
    send("HGETALL", key)
    asList.map(_.grouped(2).toList.flatMap {
      case List(Some(f), Some(v)) => List((f,v))
      case _ => Nil
    }.toMap)
  }
}