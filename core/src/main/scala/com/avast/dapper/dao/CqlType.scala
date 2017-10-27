package com.avast.dapper.dao

sealed trait CqlType {}

object CqlType {

  // all following types arecase class with no param intentionally - it't used like a better enum both into Scala and Java, case object cannot be used

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

}
