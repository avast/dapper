package com.avast.dapper.dao

import scala.reflect.ClassTag

trait CassandraPrimaryKey {
  def key: Seq[Any]
}

case class CassandraPlainKey[K: ClassTag](primaryKey: K) extends CassandraPrimaryKey {
  //  private val codec = implicitly[ScalaCodec[K, Integer]]

  override val key: Seq[Any] = Seq(primaryKey) //.map(codec.toObject)

  override def toString: String = s"($primaryKey)"
}

//
//class CassandraCompoundKey private(keys: Map[Any, ScalaCodec[_]]) extends CassandraPrimaryKey {
//  override val key: Seq[Object] = keys.map { case (value, codec) => codec.asInstanceOf[ScalaCodec[Any]].toObject(value) }.toSeq
//
//  override def toString: String = keys.keys.mkString("(", ", ", ")")
//}
//
//object CassandraCompoundKey {
//  def apply[K: ScalaCodec, K2: ScalaCodec](key1: K, key2: K2): CassandraCompoundKey = {
//    new CassandraCompoundKey(Map(
//      key1 -> implicitly[ScalaCodec[K]],
//      key2 -> implicitly[ScalaCodec[K2]]
//    ))
//  }

//
//  def apply[K: ScalaCodec, K2: ScalaCodec, K3: ScalaCodec](key1: K, key2: K2, key3: K3): CassandraCompoundKey = {
//    new CassandraCompoundKey(Seq(key1, key2, key3))
//  }
//
//  def apply[K: ScalaCodec, K2: ScalaCodec, K3: ScalaCodec, K4: ScalaCodec](key1: K, key2: K2, key3: K3, key4: K4): CassandraCompoundKey = {
//    new CassandraCompoundKey(Seq(key1, key2, key3, key4))
//  }
//
//  def apply[K: ScalaCodec, K2: ScalaCodec, K3: ScalaCodec, K4: ScalaCodec, K5: ScalaCodec](key1: K,
//                                                                                      key2: K2,
//                                                                                      key3: K3,
//                                                                                      key4: K4,
//                                                                                      key5: K5): CassandraCompoundKey = {
//    new CassandraCompoundKey(Seq(key1, key2, key3, key4, key5))
//  }
//
//  def apply[K: ScalaCodec, K2: ScalaCodec, K3: ScalaCodec, K4: ScalaCodec, K5: ScalaCodec, K6: ScalaCodec](key1: K,
//                                                                                                     key2: K2,
//                                                                                                     key3: K3,
//                                                                                                     key4: K4,
//                                                                                                     key5: K5,
//                                                                                                     key6: K6): CassandraCompoundKey = {
//    new CassandraCompoundKey(Seq(key1, key2, key3, key4, key5, key6))
//  }
//}
