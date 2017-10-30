package com.avast.dapper.dao

import java.time.{Duration, Instant}

import com.datastax.driver.core.ConsistencyLevel

import scala.concurrent.Future

trait Dao[PrimaryKey, Entity <: CassandraEntity[PrimaryKey]] {
  def tableName: String

  def get(primaryKey: PrimaryKey, queryOptions: ReadOptions = ReadOptions.Default): Future[Option[Entity]]

  def save(instance: Entity, queryOptions: WriteOptions = WriteOptions.Default): Future[Unit]

  def delete(instance: Entity, queryOptions: DeleteOptions = DeleteOptions.Default): Future[Unit]
}

case class ReadOptions(consistencyLevel: Option[ConsistencyLevel] = None)

case class WriteOptions(ttl: Option[Duration] = None, timestamp: Option[Instant] = None, consistencyLevel: Option[ConsistencyLevel] = None, ifNotExist: Boolean = false)

case class DeleteOptions(timestamp: Option[Instant] = None, consistencyLevel: Option[ConsistencyLevel] = None, ifExists: Boolean = false)

object ReadOptions {
  val Default: ReadOptions = ReadOptions()
}

object WriteOptions {
  val Default: WriteOptions = WriteOptions()
}

object DeleteOptions {
  val Default: DeleteOptions = DeleteOptions()
}
