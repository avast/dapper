package com.avast.dapper.dao

import com.datastax.driver.core.TypeCodec

sealed trait CqlType {}

object CqlType {

  // all following types are case class with no param intentionally - it't used like a better enum both into Scala and Java, case object cannot be used

  case class VarChar() extends CqlType

  case class Ascii() extends CqlType

  case class Int() extends CqlType

  case class UUID() extends CqlType

  case class TimeUUID() extends CqlType

  case class Boolean() extends CqlType

  case class Blob() extends CqlType

  case class Double() extends CqlType

  case class Float() extends CqlType

  case class Date() extends CqlType

  case class Timestamp() extends CqlType

  case class List[A <: CqlType]() extends CqlType

  case class Set[A <: CqlType]() extends CqlType

  case class Map[K <: CqlType, V <: CqlType]() extends CqlType

  case class Tuple2[A1 <: CqlType, A2 <: CqlType]() extends CqlType

}
