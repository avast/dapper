package com.avast.dapper.dao

import java.util.UUID

import com.datastax.driver.core.TypeCodec

abstract class ScalaCodec[T, JavaT, DbType <: CqlType](val javaTypeCodec: TypeCodec[JavaT]) {

  def toObject(v: T): JavaT

  def fromObject(o: JavaT): T
}

object ScalaCodec {

  def identity[A, CT](typeCodec: TypeCodec[A]): ScalaCodec[A, A, CT] = {
    new ScalaCodec[A, A, CT](typeCodec) {
      override def toObject(v: A): A = v

      override def fromObject(o: A): A = o
    }
  }

  implicit val intCodec = new ScalaCodec[Int, Integer, CqlType.Int](TypeCodec.cint()) {

    override def toObject(v: Int): Integer = v

    override def fromObject(o: Integer): Int = o
  }

  implicit val stringCodec = ScalaCodec.identity[String, CqlType.VarChar](TypeCodec.varchar())
  implicit val uuid = ScalaCodec.identity[UUID, CqlType.UUID](TypeCodec.uuid())
  implicit val timeUuid = ScalaCodec.identity[UUID, CqlType.TimeUUID](TypeCodec.timeUUID())
}
