package com.avast.dapper.dao

sealed trait CqlType {}

object CqlType {

  case class VarChar() extends CqlType

  case class Int() extends CqlType

  case class UUID() extends CqlType

  case class TimeUUID() extends CqlType

}
