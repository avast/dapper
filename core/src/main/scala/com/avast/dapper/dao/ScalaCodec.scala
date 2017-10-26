package com.avast.dapper.dao

import com.datastax.driver.core.TypeCodec

import scala.reflect.ClassTag

trait ScalaCodec[T] {

  type JavaT <: Object

  protected val ct: ClassTag[T]

  def toObject(v: T): JavaT

  def fromObject(o: JavaT): T

  def javaTypeCodec: TypeCodec[JavaT]
}
