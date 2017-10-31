package com.avast.dapper

import java.nio.file.{Files, Paths}

import com.datastax.driver.core.{Session, SimpleStatement}
import org.apache.commons.lang3.StringUtils

trait CassandraTestBase extends TestBase {

  protected val cassandraSession: Session = {
    // init the DB
    val session = ClusterBuilder.fromConfig(config.getConfig("cassandra")).withoutMetrics().build().connect()
    prepareDatabase(session)
    session.close()

    ClusterBuilder.fromConfig(config.getConfig("cassandra")).withoutMetrics().build().connect()
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
