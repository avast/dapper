package com.avast

import com.avast.dapper.dao.{CassandraDao, CassandraEntity}
import com.datastax.driver.core.Session

import scala.language.experimental.macros

package object dapper {

  implicit class DatastaxSession(val session: Session) extends AnyVal {
    def createDaoFor[PrimaryKey, Entity <: CassandraEntity[PrimaryKey]]: CassandraDao[PrimaryKey, Entity] =
      macro Macros.createDao[PrimaryKey, Entity]
  }

}
