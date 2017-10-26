package com.avast.dapper.dao

trait CassandraEntity[PrimaryKey] {

  def primaryKey: PrimaryKey
}
