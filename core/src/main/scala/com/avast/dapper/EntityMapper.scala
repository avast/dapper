package com.avast.dapper

import com.datastax.driver.core.{ConsistencyLevel, Row, Statement}

import scala.annotation.implicitNotFound

@implicitNotFound(
  "Could not find an instance of EntityMapper for entity ${Entity} and primary key ${PrimaryKey}, try to import or define one")
trait EntityMapper[PrimaryKey, Entity <: CassandraEntity[PrimaryKey]] {

  def tableName: String

  def primaryKeyPattern: String

  def getPrimaryKey(instance: Entity): PrimaryKey

  def convertPrimaryKey(k: PrimaryKey): Seq[Object]

  def extract(r: Row): Entity

  def save(tableName: String, e: Entity, writeOptions: WriteOptions): Statement

  def defaultReadConsistencyLevel: Option[ConsistencyLevel]

  def defaultWriteConsistencyLevel: Option[ConsistencyLevel]

  def defaultWriteTTL: Option[Int]
}
