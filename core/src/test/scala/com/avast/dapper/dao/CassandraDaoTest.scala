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

    case class Location(latitude: Float, longitude: Float, accuracy: Int)

    @Table(name = "test")
    case class DbRow(@PartitionKey(order = 0) id: Int,
                     @PartitionKey(order = 1) @Column(cqlType = classOf[CqlType.TimeUUID]) created: UUID,
                     @Column(cqlType = classOf[CqlType.Map[String, String]]) params: Map[String, String],
                     @Column(cqlType = classOf[CqlType.List[String]]) names: Seq[String],
                     @Column(cqlType = classOf[CqlType.Set[String]]) ints: Set[Int],
                     @Column(cqlType = classOf[CqlType.Ascii]) value: String,
                     location: Location,
                     valueOpt: Option[String])
        extends CassandraEntity[(Int, UUID)]

    implicit val mapper: EntityMapper[(Int, UUID), DbRow] = new EntityMapper[(Int, UUID), DbRow] {

      val c1 = implicitly[ScalaCodec[Int, Integer, CqlType.Int]]
      val c2 = implicitly[ScalaCodec[UUID, UUID, CqlType.TimeUUID]]
      val c3 = ScalaCodec.map[String, String, CqlType.Ascii, String, String, CqlType.Ascii]
      val c4 = ScalaCodec.list(ScalaCodec.varchar)
      val c5 = ScalaCodec.set(ScalaCodec.int)
      val c6 = implicitly[ScalaCodec[String, String, CqlType.VarChar]]

      val c7_1 = implicitly[ScalaCodec[Float, java.lang.Float, CqlType.Float]]
      val c7_2 = implicitly[ScalaCodec[Float, java.lang.Float, CqlType.Float]]
      val c7_3 = implicitly[ScalaCodec[Int, Integer, CqlType.Int]]

      val c8 = implicitly[ScalaCodec[Option[String], String, CqlType.VarChar]]

      CodecRegistry.DEFAULT_INSTANCE.register(c1.javaTypeCodec,
                                              c2.javaTypeCodec,
                                              c3.javaTypeCodec,
                                              c4.javaTypeCodec,
                                              c5.javaTypeCodec,
                                              c6.javaTypeCodec)

      override def primaryKeyPattern: String = "id = ? and created = ?"

      def getPrimaryKey(instance: DbRow): (Int, UUID) = (instance.id, instance.created)

      override def convertPrimaryKey(k: (Int, UUID)): Seq[Object] = {
        val (k1, k2) = k

        Seq(c1.toObject(k1), c2.toObject(k2))
      }

      override def extract(r: ResultSet): DbRow = {
        val row = r.one()

        val locData = row.getUDTValue("location")

        val location = Location(
          latitude = c7_1.fromObject(locData.get("latitude", c7_1.javaTypeCodec)),
          longitude = c7_2.fromObject(locData.get("longitude", c7_2.javaTypeCodec)),
          accuracy = c7_3.fromObject(locData.get("accuracy", c7_3.javaTypeCodec))
        )

        DbRow(
          c1.fromObject(row.get("id", c1.javaTypeCodec)),
          c2.fromObject(row.get("created", c2.javaTypeCodec)),
          c3.fromObject(row.get("params", c3.javaTypeCodec)),
          c4.fromObject(row.get("names", c4.javaTypeCodec)),
          c5.fromObject(row.get("ints", c5.javaTypeCodec)),
          c6.fromObject(row.get("value", c6.javaTypeCodec)),
          location,
          c8.fromObject(row.get("valueOpt", c8.javaTypeCodec)),
        )
      }

      override def save(tableName: String, e: DbRow): Statement = {
        new SimpleStatement(
          s"insert into $tableName (id, created, params, names, ints, value, location) values (?, ?, ?, ?, ?, ?, {latitude: ?, longitude: ?, accuracy: ?})",
          c1.toObject(e.id),
          c2.toObject(e.created),
          c3.toObject(e.params),
          c4.toObject(e.names),
          c5.toObject(e.ints),
          c6.toObject(e.value),
          c7_1.toObject(e.location.latitude),
          c7_2.toObject(e.location.longitude),
          c7_3.toObject(e.location.accuracy)
        )
      }
    }

    val dao = new CassandraDao[(Int, UUID), DbRow]("test", cassandra.underlying)

    val randomRow = DbRow(
      id = Random.nextInt(1000),
      created = UUIDs.timeBased(),
      value = randomString(10),
      params = Map(randomString(5) -> randomString(5), randomString(5) -> randomString(5)),
      names = Seq(randomString(5), randomString(5)),
      ints = Set(Random.nextInt(1000), Random.nextInt(1000), Random.nextInt(1000)),
      location = Location(Random.nextFloat(), Random.nextFloat(), Random.nextInt(100)),
      valueOpt = None
    )

    dao.save(randomRow).futureValue

    assertResult(Some(randomRow))(dao.get((randomRow.id, randomRow.created)).futureValue)
  }

}
