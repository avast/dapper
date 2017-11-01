package com.avast.dapper

import java.time.{Duration, Instant}

import com.datastax.driver.core.ConsistencyLevel

case class ReadOptions(consistencyLevel: Option[ConsistencyLevel] = None)

case class WriteOptions(ttl: Option[Duration] = None,
                        timestamp: Option[Instant] = None,
                        consistencyLevel: Option[ConsistencyLevel] = None,
                        serialConsistencyLevel: Option[ConsistencyLevel] = None,
                        ifNotExist: Boolean = false)

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
