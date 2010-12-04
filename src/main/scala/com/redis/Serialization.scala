package com.redis.serialization

object Format {
  def apply(f: PartialFunction[Any, Any]): Format = new Format(f)

  implicit val default: Format = new Format(Map.empty)

  def formatDouble(d: Double, inclusive: Boolean = true) =
    (if (inclusive) ("") else ("(")) + {
      if (d.isInfinity) {
        if (d > 0.0) "+inf" else "-inf"
      } else {
        d.toString
      }
    }

}

class Format(val format: PartialFunction[Any, Any]) {
  def apply(in: Any): Array[Byte] =
    (if (format.isDefinedAt(in)) (format(in)) else (in)) match {
      case b: Array[Byte] => b
      case d: Double => Format.formatDouble(d, true).getBytes("UTF-8")
      case x => x.toString.getBytes("UTF-8")
    }

  def orElse(that: Format): Format = Format(format orElse that.format)

  def orElse(that: PartialFunction[Any, Any]): Format = Format(format orElse that)
}

object Parse {
  def apply[T](f: (Array[Byte]) => T) = new Parse[T](f)

  object Implicits {
    implicit val parseString = Parse[String](new String(_, "UTF-8"))
    implicit val parseByteArray = Parse[Array[Byte]](x => x)
    implicit val parseInt = Parse[Int](new String(_, "UTF-8").toInt)
    implicit val parseLong = Parse[Long](new String(_, "UTF-8").toLong)
    implicit val parseDouble = Parse[Double](new String(_, "UTF-8").toDouble)
  }

  implicit val parseDefault = Parse[String](new String(_, "UTF-8"))
}

class Parse[A](val f: (Array[Byte]) => A) extends Function1[Array[Byte], A] {
  def apply(in: Array[Byte]): A = f(in)
}
