package com.avast.dapper.dao

import com.datastax.driver.core._
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.{ExecutionContextExecutor, Future}

class CassandraDao[PrimaryKey, Entity <: CassandraEntity[PrimaryKey]](session: Session)(
    implicit entityMapper: EntityMapper[PrimaryKey, Entity],
    ec: ExecutionContextExecutor)
    extends Dao[PrimaryKey, Entity]
    with LazyLogging {

  import CassandraDao._

  override def tableName: String = entityMapper.tableName

  override def get(primaryKey: PrimaryKey, queryOptions: ReadOptions = ReadOptions.Default): Future[Option[Entity]] = {
    // TODO options
    logger.debug(s"Querying $tableName with primary key $primaryKey and $queryOptions")

    val convertedKey = entityMapper.convertPrimaryKey(primaryKey)
    val q = new SimpleStatement(s"select * from $tableName where ${entityMapper.primaryKeyPattern}", convertedKey: _*)

    execute(q)
      .map { r =>
        if (r.isExhausted) None else Option(entityMapper.extract(r.one()))
      }
  }

  override def save(instance: Entity, queryOptions: WriteOptions = WriteOptions.Default): Future[Unit] = {
    logger.debug(s"Saving to $tableName with primary key ${entityMapper.getPrimaryKey(instance)} and $queryOptions")
    execute(entityMapper.save(tableName, instance, queryOptions)).map(toUnit)
  }

  override def delete(instance: Entity, queryOptions: DeleteOptions = DeleteOptions.Default): Future[Unit] = {
    // TODO options
    val primaryKey = entityMapper.getPrimaryKey(instance)

    logger.debug(s"Deleting from $tableName with primary key $primaryKey and $queryOptions")

    val convertedKey = entityMapper.convertPrimaryKey(primaryKey)
    val q = new SimpleStatement(s"delete from $tableName where ${entityMapper.primaryKeyPattern}", convertedKey)

    execute(q).map(toUnit)
  }

  private def execute(st: Statement): Future[ResultSet] = {
    logger.debug(s"Executing: $st")
    session.executeAsync(st).asScala
  }
}

object CassandraDao {
  private val toUnit = (_: AnyRef) => ()
}
