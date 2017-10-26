package com.avast.dapper.dao

import scala.concurrent.Future

trait Dao[PrimaryKey, Entity <: CassandraEntity[PrimaryKey]] {
  def tableName: String

  def get(primaryKey: PrimaryKey): Future[Option[Entity]]

  def save(instance: Entity): Future[Unit]

  def delete(instance: Entity): Future[Unit]
}
