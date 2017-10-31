package com.avast.dapper

import com.datastax.driver.core.{ConsistencyLevel, Row}

import scala.annotation.implicitNotFound

@implicitNotFound(
  "Could not find an instance of EntityMapper for entity ${Entity} and primary key ${PrimaryKey}, try to import or define one")
trait EntityMapper[PrimaryKey, Entity <: CassandraEntity[PrimaryKey]] {

  def getWhereParams(k: PrimaryKey): Map[String, Object]

  def getPrimaryKey(instance: Entity): PrimaryKey

  def extract(r: Row): Entity

  def getFields(instance: Entity): Map[String, Object]

  def defaultReadConsistencyLevel: Option[ConsistencyLevel]

  def defaultWriteConsistencyLevel: Option[ConsistencyLevel]

  def defaultWriteTTL: Option[Int]
}
