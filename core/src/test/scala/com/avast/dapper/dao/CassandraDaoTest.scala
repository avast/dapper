package com.avast.dapper.dao

import java.util.UUID

import com.avast.dapper.CassandraTestBase
import com.datastax.driver.core._
import com.datastax.driver.core.utils.UUIDs

import scala.util.Random

class CassandraDaoTest extends CassandraTestBase {
  implicit val ec = scala.concurrent.ExecutionContext.global

  override protected def dbCommands: Seq[String] = Seq.empty

  test("manual mapper") {
    @Table(name = "test")
    case class DbRow(@PartitionKey(order = 0) id: Int,
                     @PartitionKey(order = 0) @Column(cqlType = CqlType.TimeUUID) created: UUID,
                     value: String)
        extends CassandraEntity[(Int, UUID)]

    implicit val mapper: EntityMapper[(Int, UUID), DbRow] = new EntityMapper[(Int, UUID), DbRow] {

      val c1 = ScalaCodecs.intCodec
      val c2 = ScalaCodecs.timeUuid
      val c3 = ScalaCodecs.stringCodec

      CodecRegistry.DEFAULT_INSTANCE.register(c1.javaTypeCodec, c2.javaTypeCodec, c3.javaTypeCodec)

      override def primaryKeyPattern: String = "id = ? and created = ?"

      def getPrimaryKey(instance: DbRow): (Int, UUID) = (instance.id, instance.created)

      override def convertPrimaryKey(k: (Int, UUID)): Seq[Object] = {
        val (k1, k2) = k

        Seq(c1.toObject(k1), c2.toObject(k2))
      }

      override def extract(r: ResultSet): DbRow = {
        val row = r.one()

        DbRow(
          c1.fromObject(row.get("id", c1.javaTypeCodec)),
          c2.fromObject(row.get("created", c2.javaTypeCodec)),
          c3.fromObject(row.get("value", c3.javaTypeCodec)),
        )
      }

      override def save(tableName: String, e: DbRow): Statement = {
        new SimpleStatement(s"insert into $tableName (id, created, value) values (?, ?, ?)",
                            c1.toObject(e.id),
                            c2.toObject(e.created),
                            c3.toObject(e.value))
      }
    }

    val dao = new CassandraDao[(Int, UUID), DbRow]("test", cassandra.underlying)

    val randomRow = DbRow(
      id = Random.nextInt(1000),
      created = UUIDs.timeBased(),
      value = randomString(10)
    )

    dao.save(randomRow).futureValue

    assertResult(Some(randomRow))(dao.get((randomRow.id, randomRow.created)).futureValue)
  }

}
