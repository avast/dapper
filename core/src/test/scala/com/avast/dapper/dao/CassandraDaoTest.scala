package com.avast.dapper.dao

import com.avast.dapper.CassandraTestBase
import com.datastax.driver.core._
import com.datastax.driver.mapping.annotations.{PartitionKey, Table}

import scala.reflect.ClassTag
import scala.util.Random

class CassandraDaoTest extends CassandraTestBase {
  implicit val ec = scala.concurrent.ExecutionContext.global

  override protected def dbCommands: Seq[String] = Seq.empty

  implicit val intCodec: ScalaCodec[Int] = new ScalaCodec[Int] {

    type JavaT = Integer

    override protected val ct: ClassTag[Int] = implicitly[ClassTag[Int]]

    override def toObject(v: Int): Integer = v

    override def fromObject(o: JavaT): Int = o

    override def javaTypeCodec: TypeCodec[Integer] = TypeCodec.cint()
  }

  implicit val stringCodec = new ScalaCodec[String] {
    type JavaT = String

    override protected val ct = implicitly[ClassTag[String]]

    override def toObject(v: String) = v

    override def fromObject(o: String) = o

    override def javaTypeCodec = TypeCodec.varchar()
  }

  test("manual mapper") {
    @Table(name = "test")
    case class DbRow(@PartitionKey id: Int, value: String) extends CassandraEntity[Int] {
      override def primaryKey: Int = id
    }

    implicit val mapper: EntityMapper[Int, DbRow] = new EntityMapper[Int, DbRow] {

      val c1 = implicitly[ScalaCodec[Int]]
      val c2 = implicitly[ScalaCodec[String]]

      CodecRegistry.DEFAULT_INSTANCE.register( // type codecs for Java types
                                              c1.javaTypeCodec,
                                              c2.javaTypeCodec)

      override def primaryKeyPattern: String = "id = ?"

      override def convertPrimaryKey(k: Int): Seq[Object] = {
        Seq(c1.toObject(k))
      }

      override def extract(r: ResultSet): DbRow = {
        val row = r.one()

        DbRow(
          c1.fromObject(row.get("id", c1.javaTypeCodec)),
          c2.fromObject(row.get("value", c2.javaTypeCodec)),
        )
      }

      override def save(tableName: String, e: DbRow): Statement = {
        new SimpleStatement(s"insert into $tableName (id, value) values (?, ?)", c1.toObject(e.id), c2.toObject(e.value))
      }
    }

    val dao = new CassandraDao[Int, DbRow]("test", cassandra.underlying)

    val randomRow = DbRow(
      id = Random.nextInt(1000),
      value = randomString(10)
    )

    dao.save(randomRow).futureValue

    assertResult(Some(randomRow))(dao.get(randomRow.id).futureValue)
  }

}
