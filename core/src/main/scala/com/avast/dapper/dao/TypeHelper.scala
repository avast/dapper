package com.avast.dapper.dao

import java.lang.reflect.Type

import scala.reflect.ClassTag

object TypeHelper {
  def toWrapper(c: Type): Type = c match {
    case java.lang.Byte.TYPE => classOf[java.lang.Byte]
    case java.lang.Short.TYPE => classOf[java.lang.Short]
    case java.lang.Character.TYPE => classOf[java.lang.Character]
    case java.lang.Integer.TYPE => classOf[java.lang.Integer]
    case java.lang.Long.TYPE => classOf[java.lang.Long]
    case java.lang.Float.TYPE => classOf[java.lang.Float]
    case java.lang.Double.TYPE => classOf[java.lang.Double]
    case java.lang.Boolean.TYPE => classOf[java.lang.Boolean]
    case java.lang.Void.TYPE => classOf[java.lang.Void]
    case cls => cls
  }

  def wrapPrimitive[A: ClassTag](v: A): Object = {
    implicitly[ClassTag[A]].runtimeClass match {
      case java.lang.Byte.TYPE => v.asInstanceOf[Byte].underlying()
      case java.lang.Short.TYPE => classOf[java.lang.Short]
      case java.lang.Character.TYPE => classOf[java.lang.Character]
      case java.lang.Integer.TYPE => v.asInstanceOf[Int].underlying()
      case java.lang.Long.TYPE => classOf[java.lang.Long]
      case java.lang.Float.TYPE => classOf[java.lang.Float]
      case java.lang.Double.TYPE => classOf[java.lang.Double]
      case java.lang.Boolean.TYPE => classOf[java.lang.Boolean]
      case java.lang.Void.TYPE => classOf[java.lang.Void]
      case _ => v.asInstanceOf[Object]
    }

  }
}
