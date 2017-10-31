package com.avast.dapper

import java.time.{Instant, Duration => JavaDuration}
import java.util.UUID

import com.avast.dapper.dao.{Column, PartitionKey, Table}
import com.datastax.driver.core._
import com.datastax.driver.core.utils.UUIDs

import scala.concurrent.ExecutionContextExecutor
import scala.util.Random

class DefaultCassandraDaoTest extends CassandraTestBase {
  implicit val ec: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global

  override protected def dbCommands: Seq[String] = Seq.empty

  test("insert, get and delete") {
    case class Location(latitude: Float, longitude: Float, @Column(name = "accuracy", cqlType = classOf[CqlType.Int]) acc: Int)

    @Table(name = "test", defaultReadConsistency = ConsistencyLevel.QUORUM, defaultWriteTTL = 60)
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
    //
    //    val dao =  {
    //      val cassandraInstance = DefaultCassandraDaoTest.this.cassandraSession;
    //      implicit private val mapper: EntityMapper[(Int, java.util.UUID), DbRow] = {
    //        final class $anon extends EntityMapper[(Int, java.util.UUID), DbRow] {
    //
    //            import com.datastax.driver.{core=>Datastax};
    //            import Datastax.querybuilder._;
    //            import Datastax.querybuilder.Select._;
    //            private val codec_location = {
    //            val userType = cassandraInstance.getCluster.getMetadata.getKeyspace(cassandraInstance.getLoggedKeyspace).getUserType("location");
    //            val udtCodec: TypeCodec[UDTValue] = CodecRegistry.DEFAULT_INSTANCE.codecFor(userType);
    //            {
    //              final class $anon extends ScalaCodec[Location, UDTValue, CqlType.UDT](udtCodec) {
    //
    //                  override def toObject(udt: Location): UDTValue = {
    //                  val builder = userType.newValue();
    //                  {
    //                    val codec_location_latitude = implicitly[ScalaCodec[Float, Float, com.avast.dapper.CqlType.Float]];
    //                    builder.set("latitude", udt.latitude, codec_location_latitude.javaTypeCodec)
    //                  };
    //                  {
    //                    val codec_location_longitude = implicitly[ScalaCodec[Float, Float, com.avast.dapper.CqlType.Float]];
    //                    builder.set("longitude", udt.longitude, codec_location_longitude.javaTypeCodec)
    //                  };
    //                  {
    //                    val codec_location_accuracy = implicitly[ScalaCodec[Int, Integer, com.avast.dapper.CqlType.Int]];
    //                    builder.set("accuracy", codec_location_accuracy.toObject( udt.accuracy), codec_location_accuracy.javaTypeCodec)
    //                  }
    //                  };
    //                  override def fromObject(o: UDTValue): Location = null
    //                  };
    //                  new $anon()
    //                  }
    //                  };
    //                  private val codec_names = ScalaCodec.list(ScalaCodec.varchar);
    //                  private val codec_params = ScalaCodec.map(ScalaCodec.ascii, ScalaCodec.ascii);
    //                  private val codec_ints = ScalaCodec.set(ScalaCodec.int);
    //                  private val codec_valueOpt = implicitly[ScalaCodec[Option[String], String, com.avast.dapper.CqlType.VarChar]];
    //                  private val codec_id = implicitly[ScalaCodec[Int, Integer, com.avast.dapper.CqlType.Int]];
    //                  private val codec_stringValue = implicitly[ScalaCodec[String, String, com.avast.dapper.CqlType.VarChar]];
    //                  private val codec_tuple = ScalaCodec.tuple2(cassandraInstance.session.getCluster.getMetadata.newTupleType(Datastax.DataType.cint(), Datastax.DataType.varchar()))(ScalaCodec.int, ScalaCodec.varchar);
    //                  private val codec_created = implicitly[ScalaCodec[java.util.UUID, java.util.UUID, com.avast.dapper.CqlType.TimeUUID]];
    //                  val defaultReadConsistencyLevel: Option[Datastax.ConsistencyLevel] = Some(Datastax.ConsistencyLevel.valueOf("QUORUM"));
    //                  val defaultWriteConsistencyLevel: Option[Datastax.ConsistencyLevel] = None;
    //                  val defaultWriteTTL: Option[Int] = Some("60".toInt);
    //                  def getWhereParams(k: (Int, java.util.UUID)): Map[String, Object] = {
    //                  import k._;
    //                  Map("id".$minus$greater(codec_id.toObject(_1)), "created".$minus$greater(codec_created.toObject(_2)))
    //                  };
    //                  def getPrimaryKey(instance: DbRow): (Int, java.util.UUID) = scala.Tuple2(instance.id, instance.created);
    //                  def extract(row: Datastax.Row): DbRow = new DbRow(params = codec_params.fromObject(row.get("params", codec_params.javaTypeCodec)), valueOpt = codec_valueOpt.fromObject(row.get("valueOpt", codec_valueOpt.javaTypeCodec)), names = codec_names.fromObject(row.get("names", codec_names.javaTypeCodec)), location = codec_location.fromObject(row.get("location", codec_location.javaTypeCodec)), id = codec_id.fromObject(row.get("id", codec_id.javaTypeCodec)), created = codec_created.fromObject(row.get("created", codec_created.javaTypeCodec)), ints = codec_ints.fromObject(row.get("ints", codec_ints.javaTypeCodec)), stringValue = codec_stringValue.fromObject(row.get("value", codec_stringValue.javaTypeCodec)), tuple = codec_tuple.fromObject(row.get("tuple", codec_tuple.javaTypeCodec)));
    //                  def getFields(e: DbRow): Map[String, Object] = Map("params".$minus$greater(codec_params.toObject(e.params)), "valueOpt".$minus$greater(codec_valueOpt.toObject(e.valueOpt)), "names".$minus$greater(codec_names.toObject(e.names)), "location".$minus$greater(codec_location.toObject(e.location)), "id".$minus$greater(codec_id.toObject(e.id)), "created".$minus$greater(codec_created.toObject(e.created)), "ints".$minus$greater(codec_ints.toObject(e.ints)), "stringValue".$minus$greater(codec_stringValue.toObject(e.stringValue)), "tuple".$minus$greater(codec_tuple.toObject(e.tuple)))
    //                  };
    //                  new $anon()
    //                  };
    //                  new DefaultCassandraDao[(Int, java.util.UUID), DbRow](cassandraInstance.session, "test")
    //                  }
    //

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

    assertResult(None)(dao.get((randomRow.id, randomRow.created)).futureValue)

    dao.save(randomRow, options).futureValue

    assertResult(Some(randomRow))(dao.get((randomRow.id, randomRow.created)).futureValue)

    dao.delete(randomRow).futureValue

    assertResult(None)(dao.get((randomRow.id, randomRow.created)).futureValue)
  }

}
