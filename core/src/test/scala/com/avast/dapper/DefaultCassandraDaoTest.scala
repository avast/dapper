package com.avast.dapper

import java.time.{Instant, Duration => JavaDuration}
import java.util.UUID

import com.avast.dapper.dao.{Column, PartitionKey, Table, UDT}
import com.datastax.driver.core._
import com.datastax.driver.core.utils.UUIDs
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.ExecutionContextExecutor
import scala.util.Random

class DefaultCassandraDaoTest extends CassandraTestBase {
  implicit val ec: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global

  override protected def dbCommands: Seq[String] = Seq.empty

  test("insert, get and delete") {

    @UDT(name = "location_type")
    case class Location(latitude: Float, longitude: Float, @Column(name = "accuracy", cqlType = classOf[CqlType.Int]) acc: Int)

    @Table(keyspace = "dapper", name = "test", defaultReadConsistency = ConsistencyLevel.QUORUM, defaultWriteTTL = 60)
    case class DbRow(
        @PartitionKey(order = 0) id: Int,
        @PartitionKey(order = 1) @Column(cqlType = classOf[CqlType.TimeUUID]) created: UUID,
        @Column(cqlType = classOf[CqlType.Map[CqlType.Ascii, CqlType.Ascii]]) params: Map[String, String], // needs expl. types - not a default string codec
        names: Seq[String],
        ints: Set[Int],
        @Column(name = "value", cqlType = classOf[CqlType.VarChar]) stringValue: String,
        @Column(cqlType = classOf[CqlType.UDT]) location: Location,
        valueOpt: Option[String],
        tuple: (Int, String)
    ) extends CassandraEntity[(Int, UUID)]

    val dao = cassandraSession.createDaoFor[(Int, UUID), DbRow]

    val randomRow = DbRow(
      id = Random.nextInt(1000),
      created = UUIDs.timeBased(),
      stringValue = randomString(10),
      params = Map(randomString(5) -> randomString(5), randomString(5) -> randomString(5)),
      names = Seq(randomString(5), randomString(5) + "ěščřžýá"),
      ints = Set(Random.nextInt(1000), Random.nextInt(1000), Random.nextInt(1000)),
      location = Location(Random.nextFloat(), Random.nextFloat(), Random.nextInt(100)),
      valueOpt = None,
      tuple = (Random.nextInt(1000), randomString(10) + "ěščřžýáí")
    )

    val options = WriteOptions(
      ttl = Some(JavaDuration.ofSeconds(20)),
      timestamp = Some(Instant.now()),
      consistencyLevel = Some(ConsistencyLevel.EACH_QUORUM)
    )

    // check not present
    assertResult(None)(dao.get((randomRow.id, randomRow.created)).futureValue)

    // save
    dao.save(randomRow, options).futureValue

    // check present
    assertResult(Some(randomRow))(dao.get((randomRow.id, randomRow.created)).futureValue)

    // delete
    dao.delete(randomRow).futureValue

    // check not present
    assertResult(None)(dao.get((randomRow.id, randomRow.created)).futureValue)

    //save
    dao.save(randomRow, WriteOptions(ttl = Some(JavaDuration.ofSeconds(2)))).futureValue

    // check present
    assertResult(Some(randomRow))(dao.get((randomRow.id, randomRow.created)).futureValue)

    eventually(timeout(Span(5, Seconds)), interval(Span(200, Millis))) {
      // check not present
      assertResult(None)(dao.get((randomRow.id, randomRow.created)).futureValue)
    }
  }

}
