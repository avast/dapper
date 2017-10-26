package com.avast.dapper.dao

import java.util.UUID

import com.datastax.driver.core.TypeCodec

abstract class ScalaCodec[T, JavaT, DbType <: CqlType](val javaTypeCodec: TypeCodec[JavaT]) {

  def toObject(v: T): JavaT

  def fromObject(o: JavaT): T
}

object ScalaCodec {
  implicit val intCodec = new ScalaCodec[Int, Integer, CqlType.Int](TypeCodec.cint()) {

    override def toObject(v: Int): Integer = v

    override def fromObject(o: Integer): Int = o
  }

  implicit val stringCodec = new ScalaCodec[String, String, CqlType.VarChar](TypeCodec.varchar()) {

    override def toObject(v: String) = v

    override def fromObject(o: String) = o
  }

  implicit val uuid = new ScalaCodec[UUID, UUID, CqlType.UUID](TypeCodec.uuid()) {

    override def toObject(v: UUID) = v

    override def fromObject(o: UUID) = o
  }

  implicit val timeUuid = new ScalaCodec[UUID, UUID, CqlType.TimeUUID](TypeCodec.timeUUID()) {

    override def toObject(v: UUID) = v

    override def fromObject(o: UUID) = o

  }
}