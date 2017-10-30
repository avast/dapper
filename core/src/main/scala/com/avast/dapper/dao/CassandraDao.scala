package com.avast.dapper.dao

import java.util.concurrent.Executor

import com.datastax.driver.core._
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.{ExecutionContext, Future}

class CassandraDao[PrimaryKey, Entity <: CassandraEntity[PrimaryKey]](session: Session)(
    implicit entityMapper: EntityMapper[PrimaryKey, Entity],
    ec: ExecutionContext,
    ex: Executor)
    extends Dao[PrimaryKey, Entity]
    with LazyLogging {

  import CassandraDao._

  override def tableName: String = entityMapper.tableName

  override def get(primaryKey: PrimaryKey, queryOptions: ReadOptions = ReadOptions.Default): Future[Option[Entity]] = {
    logger.debug(s"Querying $tableName with primary key $primaryKey and $queryOptions")

    val convertedKey = entityMapper.convertPrimaryKey(primaryKey)
    val st = new SimpleStatement(s"select * from $tableName where ${entityMapper.primaryKeyPattern}", convertedKey: _*)

    queryOptions.consistencyLevel
      .orElse(entityMapper.defaultWriteConsistencyLevel)
      .foreach(st.setConsistencyLevel)

    execute(st)
      .map { r =>
        if (r.isExhausted) None else Option(entityMapper.extract(r.one()))
      }
  }

  override def save(instance: Entity, queryOptions: WriteOptions = WriteOptions.Default): Future[Unit] = {
    logger.debug(s"Saving to $tableName with primary key ${entityMapper.getPrimaryKey(instance)} and $queryOptions")
    execute(entityMapper.save(tableName, instance, queryOptions)).map(toUnit)
  }

  override def delete(instance: Entity, queryOptions: DeleteOptions = DeleteOptions.Default): Future[Unit] = {
    def decorateWithOptions(query: String): String = {
      val timestamp = queryOptions.timestamp.map(_.toEpochMilli * 1000).map(" USING TIMESTAMP " + _)
      val ifExists = if (queryOptions.ifExists) " IF EXISTS" else ""

      query + timestamp + ifExists
    }

    val primaryKey = entityMapper.getPrimaryKey(instance)

    logger.debug(s"Deleting from $tableName with primary key $primaryKey and $queryOptions")

    val convertedKey = entityMapper.convertPrimaryKey(primaryKey)
    val query = decorateWithOptions {
      s"delete from $tableName where ${entityMapper.primaryKeyPattern}"
    }

    val st = new SimpleStatement(query, convertedKey)

    queryOptions.consistencyLevel
      .orElse(entityMapper.defaultWriteConsistencyLevel)
      .foreach(st.setConsistencyLevel)

    execute(st).map(toUnit)
  }

  private def execute(st: Statement): Future[ResultSet] = {
    logger.debug(s"Executing: $st")
    session.executeAsync(st).asScala
  }
}

object CassandraDao {
  private val toUnit = (_: AnyRef) => ()
}
