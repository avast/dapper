package com.avast.dapper

import scala.concurrent.Future

trait CassandraDao[PrimaryKey, Entity <: CassandraEntity[PrimaryKey]] {
  def keySpace: String

  def tableName: String

  def get(primaryKey: PrimaryKey, queryOptions: ReadOptions = ReadOptions.Default): Future[Option[Entity]]

  def save(instance: Entity, queryOptions: WriteOptions = WriteOptions.Default): Future[Unit]

  def delete(instance: Entity, queryOptions: DeleteOptions = DeleteOptions.Default): Future[Unit]
}
