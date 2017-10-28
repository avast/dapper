package com.avast.dapper

import java.nio.file.{Files, Paths}

import com.avast.utils2.datastax.{Cassandra => UtilsCassandra, ClusterBuilder}
import com.datastax.driver.core.{Session, SimpleStatement}
import org.apache.commons.lang3.StringUtils

import scala.concurrent.ExecutionContext

trait CassandraTestBase extends TestBase {

  protected val cassandra: UtilsCassandra = {
    val executionContext = scala.concurrent.ExecutionContext.global

    // init the DB
    val session = ClusterBuilder.fromConfig(config.getConfig("cassandra")).withoutMetrics().build().connect()
    prepareDatabase(session)
    session.close()

    UtilsCassandra.fromConfig(config.getConfig("cassandra"))(executionContext)
  }

  protected def dbCommands: Seq[String]

  private def prepareDatabase(session: Session): Unit = {
    val scripts = Seq("scheme-test.cql")

    val cqls = scripts
      .flatMap { scriptName =>
        val b = Files.readAllBytes(Paths.get(getClass.getClassLoader.getResource(scriptName).toURI))

        new String(b).split("\n")
      }
      .filter(StringUtils.isNotBlank) ++ dbCommands

    cqls.foreach { cql =>
      session.execute(new SimpleStatement(cql))
    }
  }
}
