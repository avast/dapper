package com.avast.dapper.dao

import java.util.UUID

import com.datastax.driver.core.TypeCodec

import scala.reflect.ClassTag

object ScalaCodecs {

  implicit val intCodec: ScalaCodec[Int] = new ScalaCodec[Int] {

    type JavaT = Integer

    override protected val ct: ClassTag[Int] = implicitly[ClassTag[Int]]

    override def toObject(v: Int): Integer = v

    override def fromObject(o: JavaT): Int = o

    override def javaTypeCodec: TypeCodec[Integer] = TypeCodec.cint()
  }

  implicit val stringCodec = new ScalaCodec[String] {
    type JavaT = String

    override protected val ct = implicitly[ClassTag[String]]

    override def toObject(v: String) = v

    override def fromObject(o: String) = o

    override def javaTypeCodec = TypeCodec.varchar()
  }

  implicit val uuid = new ScalaCodec[UUID] {

    type JavaT = UUID

    override protected val ct = implicitly[ClassTag[UUID]]

    override def toObject(v: UUID) = v

    override def fromObject(o: UUID) = o

    override def javaTypeCodec = TypeCodec.uuid()
  }

  implicit val timeUuid = new ScalaCodec[UUID] {

    type JavaT = UUID

    override protected val ct = implicitly[ClassTag[UUID]]

    override def toObject(v: UUID) = v

    override def fromObject(o: UUID) = o

    override def javaTypeCodec = TypeCodec.timeUUID()
  }
}
