package com.avast.dapper

import java.util.concurrent.Executor

import com.datastax.driver.core._
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

class DefaultCassandraDao[PrimaryKey, Entity <: CassandraEntity[PrimaryKey]](
    session: Session,
    override val keySpace: String,
    override val tableName: String)(implicit entityMapper: EntityMapper[PrimaryKey, Entity], ec: ExecutionContext, ex: Executor)
    extends CassandraDao[PrimaryKey, Entity]
    with LazyLogging {

  import DefaultCassandraDao._

  override def get(primaryKey: PrimaryKey, queryOptions: ReadOptions = ReadOptions.Default): Future[Option[Entity]] = {
    logger.debug(s"Querying $tableName with primary key $primaryKey and $queryOptions")

    val st = QueryBuilder.select().from(keySpace, tableName)

    entityMapper
      .getWhereParams(primaryKey)
      .map { case (name, value) => QueryBuilder.eq(name, value) }
      .foldLeft(st.where())(_ and _)

    queryOptions.consistencyLevel
      .orElse(entityMapper.defaultReadConsistencyLevel)
      .foreach(st.setConsistencyLevel)

    execute(st)
      .map { r =>
        if (r.isExhausted) None else Option(entityMapper.extract(r.one()))
      }
  }

  override def save(instance: Entity, queryOptions: WriteOptions = WriteOptions.Default): Future[Unit] = {
    logger.debug(s"Saving to $tableName with primary key ${entityMapper.getPrimaryKey(instance)} and $queryOptions")

    val st = QueryBuilder.insertInto(keySpace, tableName)

    val (names, values) = entityMapper.getFields(instance).unzip
    st.values(names.toArray, values.toArray)

    if (queryOptions.ifNotExist) st.ifNotExists()

    queryOptions.consistencyLevel
      .orElse(entityMapper.defaultWriteConsistencyLevel)
      .foreach(st.setConsistencyLevel)

    queryOptions.timestamp.map(_.toEpochMilli * 1000).foreach(st.setDefaultTimestamp)
    queryOptions.ttl.map(_.getSeconds).foreach(s => st.using(QueryBuilder.ttl(s.toInt)))

    execute(st).map(toUnit)
  }

  override def delete(instance: Entity, queryOptions: DeleteOptions = DeleteOptions.Default): Future[Unit] = {
    val primaryKey = entityMapper.getPrimaryKey(instance)

    logger.debug(s"Deleting from $tableName with primary key $primaryKey and $queryOptions")

    val st = QueryBuilder.delete().from(keySpace, tableName)

    entityMapper
      .getWhereParams(primaryKey)
      .map { case (name, value) => QueryBuilder.eq(name, value) }
      .foldLeft(st.where())(_ and _)

    if (queryOptions.ifExists) st.ifExists()

    queryOptions.consistencyLevel
      .orElse(entityMapper.defaultWriteConsistencyLevel)
      .foreach(st.setConsistencyLevel)

    queryOptions.timestamp.map(_.toEpochMilli * 1000).foreach(st.setDefaultTimestamp)

    execute(st).map(toUnit)
  }

  private def execute(st: Statement): Future[ResultSet] = {
    logger.debug(s"Executing: $st")
    session.executeAsync(st).asScala
  }
}

object DefaultCassandraDao {
  private val toUnit = (_: AnyRef) => ()
}
