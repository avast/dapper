package com.avast.dapper

sealed trait CqlType

object CqlType {

  // all following types are case class with no param intentionally - it't used like a better enum both into Scala and Java, case object cannot be used

  trait VarChar  extends CqlType

  trait Ascii  extends CqlType

  trait Int  extends CqlType

  trait UUID  extends CqlType

  trait TimeUUID  extends CqlType

  trait Boolean  extends CqlType

  trait Blob  extends CqlType

  trait Double  extends CqlType

  trait Float  extends CqlType

  trait Date  extends CqlType

  trait Timestamp  extends CqlType

  trait List[A <: CqlType]  extends CqlType

  trait Set[A <: CqlType]  extends CqlType

  trait Map[K <: CqlType, V <: CqlType]  extends CqlType

  trait Tuple2[A1 <: CqlType, A2 <: CqlType]  extends CqlType

  trait UDT  extends CqlType

}
