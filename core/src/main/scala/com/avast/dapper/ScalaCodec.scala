package com.avast.dapper

import java.nio.ByteBuffer
import java.time.Instant
import java.util.{Date, UUID}
import java.{lang, util}

import com.datastax.driver.core.{TupleType, TupleValue, TypeCodec}
import com.datastax.driver.{core => Datastax}

import scala.annotation.implicitNotFound
import scala.collection.JavaConverters._
import scala.language.implicitConversions

@implicitNotFound(
  "Could not find an instance of ScalaCodec for CQL type ${DbType}, Scala type ${T}, Java type ${JavaT}, try to import or define one")
abstract class ScalaCodec[T, JavaT, DbType <: CqlType](val javaTypeCodec: TypeCodec[JavaT]) {

  def toObject(v: T): JavaT

  def fromObject(o: JavaT): T
}

// format: OFF
object ScalaCodec {

  def simple[A, B, CT <: CqlType](typeCodec: TypeCodec[B], f1: A => B, f2: B => A): ScalaCodec[A, B, CT] = {
    new ScalaCodec[A, B, CT](typeCodec) {
      override def toObject(v: A): B = f1(v)

      override def fromObject(o: B): A = f2(o)
    }
  }

  def identity[A, CT <: CqlType](typeCodec: TypeCodec[A]): ScalaCodec[A, A, CT] = simple[A, A, CT](typeCodec, Predef.identity, Predef.identity)

  implicit def option[A, B, CT <: CqlType](implicit codec: ScalaCodec[A, B, CT]): ScalaCodec[Option[A], B, CT] = new ScalaCodec[Option[A], B, CT](codec.javaTypeCodec) {
    override def toObject(v: Option[A]): B = v.map(codec.toObject).getOrElse(null.asInstanceOf[B])

    override def fromObject(o: B): Option[A] = Option(o).map(codec.fromObject)
  }

  // TODO add all types

  implicit val int: ScalaCodec[Int, Integer, CqlType.Int] = ScalaCodec.simple[Int, Integer, CqlType.Int](TypeCodec.cint(), int2Integer, Integer2int)
  implicit val double: ScalaCodec[Double, lang.Double, CqlType.Double] = ScalaCodec.simple[Double, lang.Double, CqlType.Double](TypeCodec.cdouble(), double2Double, Double2double)
  implicit val float: ScalaCodec[Float, lang.Float, CqlType.Float] = ScalaCodec.simple[Float, lang.Float, CqlType.Float](TypeCodec.cfloat(), float2Float, Float2float)
  implicit val boolean: ScalaCodec[Boolean, lang.Boolean, CqlType.Boolean] = ScalaCodec.simple[Boolean, lang.Boolean, CqlType.Boolean](TypeCodec.cboolean(), boolean2Boolean, Boolean2boolean)
  implicit val varchar: ScalaCodec[String, String, CqlType.VarChar] = ScalaCodec.identity[String, CqlType.VarChar](TypeCodec.varchar())
  implicit val ascii: ScalaCodec[String, String, CqlType.Ascii] = ScalaCodec.identity[String, CqlType.Ascii](TypeCodec.ascii())
  implicit val uuid: ScalaCodec[UUID, UUID, CqlType.UUID] = ScalaCodec.identity[UUID, CqlType.UUID](TypeCodec.uuid())
  implicit val timeUuid: ScalaCodec[UUID, UUID, CqlType.TimeUUID] = ScalaCodec.identity[UUID, CqlType.TimeUUID](TypeCodec.timeUUID())

  implicit val timestamp: ScalaCodec[Instant, Date, CqlType.Timestamp] = ScalaCodec.simple[Instant, Date, CqlType.Timestamp](TypeCodec.timestamp(), i => new Date(i.toEpochMilli), _.toInstant)
  implicit val date: ScalaCodec[java.time.LocalDate, Datastax.LocalDate, CqlType.Date] = new ScalaCodec[java.time.LocalDate, Datastax.LocalDate, CqlType.Date](TypeCodec.date()) {
    override def toObject(v: java.time.LocalDate): Datastax.LocalDate = {
      Datastax.LocalDate.fromYearMonthDay(v.getYear, v.getMonthValue, v.getDayOfMonth)
    }

    override def fromObject(v: Datastax.LocalDate): java.time.LocalDate = {
      java.time.LocalDate.of(v.getYear, v.getMonth, v.getDay)
    }
  }

  implicit val blob: ScalaCodec[Array[Byte], ByteBuffer, CqlType.Blob] = new ScalaCodec[Array[Byte], ByteBuffer, CqlType.Blob](TypeCodec.blob()) {
    override def toObject(v: Array[Byte]): ByteBuffer = ByteBuffer.wrap(v)

    override def fromObject(bb: ByteBuffer): Array[Byte] = {
      val b = new Array[Byte](bb.remaining)
      bb.get(b)
      b
    }
  }

  def list[A, B, CT <: CqlType](elemCodec: ScalaCodec[A, B, CT]): ScalaCodec[Seq[A], java.util.List[B], CqlType.List[CT]] = ScalaCodec.simple[Seq[A], java.util.List[B], CqlType.List[CT]](TypeCodec.list[B](elemCodec.javaTypeCodec), _.map(elemCodec.toObject).asJava, _.asScala.map(elemCodec.fromObject))

  def set[A, B, CT <: CqlType](elemCodec: ScalaCodec[A, B, CT]): ScalaCodec[Set[A], java.util.Set[B], CqlType.Set[CT]] = ScalaCodec.simple[Set[A], java.util.Set[B], CqlType.Set[CT]](TypeCodec.set[B](elemCodec.javaTypeCodec), _.map(elemCodec.toObject).asJava, _.asScala.map(elemCodec.fromObject).toSet)

  def map[K, KJ, KCT <: CqlType, V, VJ, VCT <: CqlType](implicit keyCodec: ScalaCodec[K, KJ, KCT], valueCodec: ScalaCodec[V, VJ, VCT]): ScalaCodec[Map[K, V], util.Map[KJ, VJ], CqlType.Map[KCT, VCT]] =
    new ScalaCodec[Map[K, V], java.util.Map[KJ, VJ], CqlType.Map[KCT, VCT]](TypeCodec.map[KJ, VJ](keyCodec.javaTypeCodec, valueCodec.javaTypeCodec)) {
      override def toObject(m: Map[K, V]): util.Map[KJ, VJ] = m.map {
        case (key, value) =>
          keyCodec.toObject(key) -> valueCodec.toObject(value)
      }.asJava

      override def fromObject(m: util.Map[KJ, VJ]): Map[K, V] = m.asScala.map {
        case (key, value) =>
          keyCodec.fromObject(key) -> valueCodec.fromObject(value)
      }.toMap
    }

  def tuple2[A1, A1J, A1CT <: CqlType, A2, A2J, A2CT <: CqlType](tupleType: TupleType)(implicit a1Codec: ScalaCodec[A1, A1J, A1CT], a2Codec: ScalaCodec[A2, A2J, A2CT]): ScalaCodec[(A1, A2), TupleValue, CqlType.Tuple2[A1CT, A2CT]] =
    new ScalaCodec[(A1, A2), TupleValue, CqlType.Tuple2[A1CT, A2CT]](TypeCodec.tuple(tupleType)) {
      override def toObject(v: (A1, A2)): TupleValue = {
        val (a1, a2) = v

        tupleType.newValue()
          .set(0, a1Codec.toObject(a1), a1Codec.javaTypeCodec)
          .set(1, a2Codec.toObject(a2), a2Codec.javaTypeCodec)
      }

      override def fromObject(o: TupleValue): (A1, A2) = {
        val a1 = a1Codec.fromObject(o.get(0, a1Codec.javaTypeCodec))
        val a2 = a2Codec.fromObject(o.get(1, a2Codec.javaTypeCodec))

        (a1, a2)
      }
    }
}
