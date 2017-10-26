package com.avast.dapper.dao

import com.datastax.driver.core._
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.{ExecutionContextExecutor, Future}

class CassandraDao[PrimaryKey, Entity <: CassandraEntity[PrimaryKey]](override val tableName: String, session: Session)(
    implicit entityMapper: EntityMapper[PrimaryKey, Entity],
    ec: ExecutionContextExecutor)
    extends Dao[PrimaryKey, Entity]
    with LazyLogging {

  import CassandraDao._

  override def get(primaryKey: PrimaryKey): Future[Option[Entity]] = {
    logger.debug(s"Selecting $tableName with primary key $primaryKey")

    val convertedKey = entityMapper.convertPrimaryKey(primaryKey)
    val q = new SimpleStatement(s"select * from $tableName where ${entityMapper.primaryKeyPattern}", convertedKey: _*)

    execute(q)
      .map { r =>
        if (r.isExhausted) None else Option(entityMapper.extract(r))
      }
  }

  override def save(instance: Entity): Future[Unit] = {
    logger.debug(s"Saving to $tableName with primary key ${entityMapper.getPrimaryKey(instance)}")
    execute(entityMapper.save(tableName, instance)).map(toUnit)
  }

  override def delete(instance: Entity): Future[Unit] = {
    val primaryKey = entityMapper.getPrimaryKey(instance)

    logger.debug(s"Deleting from $tableName with primary key $primaryKey")

    val convertedKey = entityMapper.convertPrimaryKey(primaryKey)
    val q = new SimpleStatement(s"delete from $tableName where ${entityMapper.primaryKeyPattern}", convertedKey)

    execute(q).map(toUnit)
  }

  private def execute(st: Statement): Future[ResultSet] = {
    session.executeAsync(st).asScala
  }
}

object CassandraDao {
  private val toUnit = (_: AnyRef) => ()
}
