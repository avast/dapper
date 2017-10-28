package com.avast.dapper.dao

import com.datastax.driver.core.{ResultSet, Statement}

import scala.annotation.implicitNotFound

@implicitNotFound(
  "Could not find an instance of EntityMapper for entity ${Entity} and primary key ${PrimaryKey}, try to import or define one")
trait EntityMapper[PrimaryKey, Entity <: CassandraEntity[PrimaryKey]] {

  def primaryKeyPattern: String

  def getPrimaryKey(instance: Entity): PrimaryKey

  def convertPrimaryKey(k: PrimaryKey): Seq[Object]

  def extract(r: ResultSet): Entity

  def save(tableName: String, e: Entity): Statement
}
