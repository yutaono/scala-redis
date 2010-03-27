package com.redis

private [redis] object Commands {

  // Response codes from the Redis server
  val ERR    = '-'
  val OK     = "OK"
  val SINGLE = '+'
  val BULK   = '$'
  val MULTI  = '*'
  val INT    = ':'

  val LS     = "\r\n"
}

import Commands._

private [redis] sealed abstract class Command(command: String)

private [redis] trait WithLS {
  def withLS(str: String, strs: String*) = 
    (str :: strs.toList) mkString(LS)
}

private [redis] case class MultiBulkCommand(command: String, key: String, values: String*) 
  extends Command(command) with WithLS { 
  override def toString: String = {
    val nelems = "%s%d".format(MULTI, values.toList.size + 2)
    val cl = "%s%d".format(BULK, command.length)
    val kl = "%s%d".format(BULK, key.length)
    val vs = values.toList.map(v => "%s%d%s%s%s".format(BULK, v.length, LS, v, LS))
    withLS(nelems, cl, command, kl, key, vs.mkString)
  }
}

private [redis] case class InlineCommand(command: String) extends Command(command) {
  override def toString: String = 
    "%s%s".format(command, LS)
}

private [redis] trait C {
  def snd(command: String, key: String, values: String*)(to: String => Unit) = {
    to(MultiBulkCommand(command, key, values:_*).toString)
  }

  def snd(command: String)(to: String => Unit) = {
    to(InlineCommand(command).toString)
  }
}

private [redis] trait Reply {

  def readLine: Option[String]
  def receive: Option[Any] = readLine match {
    case None => None
    case Some(line) =>
      line.toList match {
        case INT :: s => integerReply(s.mkString)
        case SINGLE :: s => singleLineReply(s.mkString)
        case BULK :: s => bulkReply(s.mkString)
        case MULTI :: s => multiBulkReply(s.mkString)
        case ERR :: s => throw new Exception(s.mkString)
        case _ => None
      }
  }

  private def integerReply(str: String) = 
    Some(Integer.parseInt(str))

  private def singleLineReply(str: String) = 
    Some(str)

  private def bulkReply(str: String) = {
    Integer.parseInt(str) match {
      case -1 => None
      case l => Some(readLine.get)
    }
  }

  private def multiBulkReply(str: String) = {
    Integer.parseInt(str) match {
      case -1 => None
      case n => 
        var l = List[Option[String]]()
        (1 to n).foreach {i =>
          receive match {
            case Some(s: String) =>
              l = l ::: List(Some(s))
            case _ => 
              l = l ::: List(None) // is required to handle nil elements in multi-bulk reply
          }
        }
        Some(l)
    }
  }
}

private [redis] trait R extends Reply {

  def asString: Option[String] = receive match {
    case Some(s: String) => Some(s)
    case _ => None
  }

  def asInt: Option[Int] = receive match {
    case Some(i: Int) => Some(i)
    case _ => None
  }

  def asBoolean: Boolean = receive match {
    case Some(i: Int) if i > 0 => true
    case Some(OK) => true
    case _ => false
  }

  def asList: Option[List[Option[String]]] = receive match {
    case Some(l: List[Option[String]]) => Some(l)
    case _ => None
  }

  def asSet: Option[Set[Option[String]]] = receive match {
    case Some(l: List[Option[String]]) => Some(Set(l: _*))
    case _ => None
  }

  def asAny: Any = receive
}

trait Protocol extends C with R
