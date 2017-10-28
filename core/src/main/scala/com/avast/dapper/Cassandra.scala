package com.avast.dapper

import com.avast.dapper.dao.{CassandraDao, CassandraEntity}
import com.datastax.driver.core.{CodecRegistry, Session}

import scala.language.experimental.macros

class Cassandra(val session: Session, val codecRegistry: CodecRegistry = CodecRegistry.DEFAULT_INSTANCE) {
  def deriveDaoFor[PrimaryKey, Entity <: CassandraEntity[PrimaryKey]]: CassandraDao[PrimaryKey, Entity] =
    macro Macros.createDao[PrimaryKey, Entity]
}
