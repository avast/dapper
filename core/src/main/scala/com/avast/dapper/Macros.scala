package com.avast.dapper

import java.nio.ByteBuffer

import com.avast.dapper.Macros.AnnotationsMap
import com.avast.dapper.dao.{Column, PartitionKey, Table, UDT}
import com.datastax.driver.{core => Datastax}

import scala.language.reflectiveCalls
import scala.reflect.macros._

class Macros(val c: whitebox.Context) {

  import c.universe._

  object CqlTypes {
    final val VarChar = typeOf[CqlType.VarChar].erasure
    final val Ascii = typeOf[CqlType.Ascii].erasure
    final val Int = typeOf[CqlType.Int].erasure
    final val UUID = typeOf[CqlType.UUID].erasure
    final val TimeUUID = typeOf[CqlType.TimeUUID].erasure
    final val Boolean = typeOf[CqlType.Boolean].erasure
    final val Blob = typeOf[CqlType.Blob].erasure
    final val Double = typeOf[CqlType.Double].erasure
    final val Float = typeOf[CqlType.Float].erasure
    final val Date = typeOf[CqlType.Date].erasure
    final val Timestamp = typeOf[CqlType.Timestamp].erasure

    final def List(t: Type): Type = getType(tq"CqlType.List[${defaultCqlType(t)}]")

    final def Set(t: Type): Type = getType(tq"CqlType.Set[${defaultCqlType(t)}]")

    final def Map(k: Type, v: Type): Type = getType(tq"CqlType.Map[${defaultCqlType(k)},${defaultCqlType(v)}]")

    final def Tuple2(a1: Type, a2: Type): Type = getType(tq"CqlType.Tuple2[${defaultCqlType(a1)},${defaultCqlType(a2)}]")

    final val UDT = typeOf[CqlType.UDT].erasure
  }

  object ScalaTypes {
    final val Option = typeOf[scala.Option[_]].erasure

    final val String = typeOf[Predef.String].erasure
    final val Int = typeOf[scala.Int].erasure
    final val UUID = typeOf[java.util.UUID].erasure
    final val Boolean = typeOf[scala.Boolean].erasure
    final val ByteArray = typeOf[Array[Byte]].erasure
    final val Double = typeOf[scala.Double].erasure
    final val Float = typeOf[scala.Float].erasure
    final val Instant = typeOf[java.time.Instant].erasure
    final val LocalDate = typeOf[java.time.LocalDate].erasure

    final val Set = typeOf[scala.collection.immutable.Set[_]].erasure
    final val Seq = typeOf[scala.collection.immutable.Seq[_]].erasure
    final val SeqMutable = typeOf[scala.collection.Seq[_]].erasure
    final val Map = typeOf[scala.Predef.Map[_, _]].erasure
    final val Tuple2 = typeOf[(_, _)].erasure
  }

  // format: OFF
  def createDao[PrimaryKey: WeakTypeTag, Entity <: CassandraEntity[PrimaryKey] : WeakTypeTag](ec: Tree, ex: Tree): Expr[DefaultCassandraDao[PrimaryKey, Entity]] = {
    // format: ON

    val primaryKeyType = weakTypeOf[PrimaryKey]
    val entityType = weakTypeOf[Entity]

    val entitySymbol = toCaseClassSymbol(entityType)

    val tableProperties = extractTableProperties(entityType, entitySymbol)
    import tableProperties._

    val entityFields: Map[Symbol, (CodecType, AnnotationsMap)] = extractFields(entityType)

    val primaryKeyFields = entityFields
      .collect {
        case (field, (_, annots)) if annots contains classOf[PartitionKey].getName =>
          annots(classOf[PartitionKey].getName)("order").toInt -> field
      }
      .toSeq
      .sortBy(_._1)
      .map(_._2)

    if (primaryKeyFields.isEmpty) {
      c.abort(c.enclosingPosition, s"Provided type ${entityType.typeSymbol} must have at least one PartitionKey annotated field")
    }

    if (primaryKeyFields.map(_.typeSignature) != primaryKeyType.typeArgs) {
      // format: OFF
      c.abort(c.enclosingPosition, s"Primary key of ${entityType.typeSymbol} ${primaryKeyFields.map(_.typeSignature).mkString("[", ", ", "]")} doesn't match declared ${primaryKeyType.typeArgs.mkString("[", ", ", "]")}")
      // format: ON
    }

    val codecs: Map[String, Tree] = entityFields.map {
      case (field, (codecType, _)) => codec(field, codecType)
    }

    val mapper =
      q"""

        private implicit val mapper: EntityMapper[$primaryKeyType, $entityType] = new EntityMapper[$primaryKeyType, $entityType] {

        import com.datastax.driver.{core => Datastax}
        import Datastax.querybuilder._
        import Datastax.querybuilder.Select._

        ..${codecs.values}

        val defaultReadConsistencyLevel: Option[Datastax.ConsistencyLevel] = $defaultReadConsistencyLevel

        val defaultWriteConsistencyLevel: Option[Datastax.ConsistencyLevel] = $defaultWriteConsistencyLevel

        val defaultSerialConsistencyLevel: Option[Datastax.ConsistencyLevel] = $defaultSerialConsistencyLevel

        val defaultWriteTTL: Option[Int] = $defaultWriteTTL

        def getWhereParams(k: $primaryKeyType): Map[String, Object] = {
          import k._

          Map(..${convertPrimaryKey(primaryKeyFields).map { case (f, v) => q"${f.name.toString} ->  $v" }})
        }

        def getPrimaryKey(instance: $entityType): $primaryKeyType = (..${primaryKeyFields.map(s => q"instance.${TermName(s.name.toString)}")})

        def extract(row: Datastax.Row): $entityType = {
          ${createExtractMethod(entitySymbol, entityFields.map { case (field, (_, annots)) => field -> fieldFinalName(field, annots) })}
        }

        def getFields(e: $entityType): Map[String, Object] = ${createGetFieldsMethod(entityFields)}

      }
      """

    // @formatter:off
    // format: OFF
    val dao =
      q"""
         {
            val cassandraInstance = $getVariable

            val keyspaceName: String = ${
              keySpace.map(n => q"$n")
              .getOrElse(q"""Option(cassandraInstance.getLoggedKeyspace).getOrElse(throw new IllegalArgumentException("Could not extract keyspace scheme; either connect Session to a keyspace or specify the keyspace in the @Table annotation"))""")
            }

            $mapper

            new DefaultCassandraDao[$primaryKeyType, $entityType](cassandraInstance.session, keyspaceName, $tableName)
         }
       """
    // format: ON
    // @formatter:on

    //    println(dao)

    c.Expr[DefaultCassandraDao[PrimaryKey, Entity]](dao)
  }

  private def extractTableProperties(entityType: Type, entitySymbol: ClassSymbol) = new {
    private val annots = getAnnotations(entitySymbol)

    private val tableAnnot: Map[String, String] = annots
      .collectFirst { case (n, params) if n == classOf[Table].getName => params }
      .getOrElse {
        c.abort(c.enclosingPosition, s"Provided type ${entityType.typeSymbol} must be annotated with @Table")
      }

    val keySpace: Option[String] = tableAnnot.get("keyspace")

    // format: OFF
    val tableName: String = tableAnnot.getOrElse("name", c.abort(c.enclosingPosition, s"Provided type ${entityType.typeSymbol} @Table annotation is missing 'name' param"))

    val defaultReadConsistencyLevel: Tree = {
      tableAnnot.get("defaultReadConsistency").flatMap(_.split(" ").tail.headOption).map(c => q"Some(Datastax.ConsistencyLevel.valueOf($c))").getOrElse(q"None")
    }

    val defaultWriteConsistencyLevel: Tree = {
      tableAnnot.get("defaultWriteConsistency").flatMap(_.split(" ").tail.headOption).map(c => q"Some(Datastax.ConsistencyLevel.valueOf($c))").getOrElse(q"None")
    }

    val defaultSerialConsistencyLevel: Tree = {
      tableAnnot.get("defaultSerialConsistency").flatMap(_.split(" ").tail.headOption).map(c => q"Some(Datastax.ConsistencyLevel.valueOf($c))").getOrElse(q"None")
    }

    val defaultWriteTTL: Tree = {
      tableAnnot.get("defaultWriteTTL").map(c => q"Some($c.toInt)").getOrElse(q"None")
    }
    // format: ON

  }

  private def extractUDTProperties(entityType: Type, entitySymbol: ClassSymbol) = new {
    private val annots = getAnnotations(entitySymbol)

    private val udtAnnot: Option[Map[String, String]] = annots
      .collectFirst { case (n, params) if n == classOf[UDT].getName => params }

    val udtName: Option[String] = udtAnnot.flatMap(_.get("name"))

  }

  private def convertPrimaryKey(primaryKeyFields: Seq[Symbol]): Map[Symbol, Tree] = {
    val withIndex = primaryKeyFields.zipWithIndex

    withIndex.map {
      case (field, index) =>
        field -> q"${TermName("codec_" + field.name)}.toObject(${TermName("_" + (index + 1))})" // for some unknown reason "k._1" cannot be used :-(
    }.toMap
  }

  private def createGetFieldsMethod(entityFields: Map[Symbol, (CodecType, AnnotationsMap)]): Tree = {
    def fieldToBindings(field: Symbol, codecType: CodecType, annots: AnnotationsMap): (String, Tree) = {
      fieldFinalName(field, annots) -> q"${TermName(s"codec_${field.name}")}.toObject(e.${TermName(field.name.toString)})"
    }

    val fields = entityFields.map {
      case (field, (codecType, annots)) =>
        val (name, value) = fieldToBindings(field, codecType, annots)
        q"$name -> $value"
    }

    q"Map(..$fields)"
  }

  private def fieldFinalName(field: Symbol, annots: AnnotationsMap): String = {
    annots
      .get(classOf[Column].getName)
      .flatMap(_.get("name"))
      .getOrElse(field.name.toString)
  }

  private def createExtractMethod(entitySymbol: TypeSymbol,
                                  entityFields: Map[Symbol, String],
                                  codecNamePrefix: String = "",
                                  rowVar: TermName = TermName("row")): Tree = {

    val fields: Iterable[Tree] = entityFields.map {
      case (field, finalFieldName) =>
        val fieldName = field.name.toString
        val codecName = TermName("codec_" + codecNamePrefix + fieldName)

        q"${TermName(fieldName)} = $codecName.fromObject($rowVar.get($finalFieldName, $codecName.javaTypeCodec))"
    }

    q"""
          new $entitySymbol(
            ..$fields
          )
     """
  }

  private def codec(field: Symbol, codecType: CodecType, namePrefix: String = ""): (String, Tree) = {
    def wrapWithVal(name: String)(codec: Tree): (String, Tree) = {
      name -> q""" private val ${TermName("codec_" + name)} = $codec """
    }

    // format: OFF
    codecType match {
      case CodecType.Simple(t, ct) => wrapWithVal(namePrefix + field.name)(q"implicitly[ScalaCodec[$t, ${javaClassForCqlType(ct)}, $ct]]")
      case CodecType.List(inT) => wrapWithVal(namePrefix + field.name.toString)(q"ScalaCodec.list(${scalaCodecForCqlType(inT)})")
      case CodecType.Set(inT) => wrapWithVal(namePrefix + field.name.toString)(q"ScalaCodec.set(${scalaCodecForCqlType(inT)})")
      case CodecType.Map(k, v) => wrapWithVal(namePrefix + field.name.toString)(q"ScalaCodec.map(${scalaCodecForCqlType(k)}, ${scalaCodecForCqlType(v)})")
      case CodecType.Tuple2(a1, a2) => wrapWithVal(namePrefix + field.name.toString)(q"ScalaCodec.tuple2(${createTupleType(a1, a2)})(${scalaCodecForCqlType(a1)}, ${scalaCodecForCqlType(a2)})")
      case CodecType.UDT(udtName, udtFields) =>
        wrapWithVal(namePrefix + field.name) {
          val udtType = field.typeSignature
          val udtCodecs = udtFields.map { case ((udtField, finalFieldName), udtCodec) =>
            (udtField, finalFieldName) -> codec(udtField, udtCodec, namePrefix + field.name + "_")
          }

          def setField(udtField: Symbol, finalFieldName: String, codecName: String): Tree = {
            q"""
              builder.set(
                $finalFieldName,
                ${TermName(s"codec_$codecName")}.toObject(udt.${TermName(udtField.name.toString)}), ${TermName(s"codec_$codecName")}.javaTypeCodec
              )
             """
          }

          // @formatter:off
          q"""
             {
               val userType = cassandraInstance.getCluster.getMetadata.getKeyspace(keyspaceName).getUserType($udtName)

               if (userType == null) throw new IllegalArgumentException("Cannot locate type '" + $udtName + "' in DB!")

               val udtCodec: TypeCodec[UDTValue] =  CodecRegistry.DEFAULT_INSTANCE.codecFor(userType)

               new ScalaCodec[$udtType, UDTValue, CqlType.UDT](udtCodec) {
                  ..${udtCodecs.map { case ((_, _), (_, codecTree)) => codecTree }}

                  override def toObject(udt: $udtType): UDTValue = {
                    val builder = userType.newValue()

                    ..${udtCodecs.map { case ((udtField, finalFieldName), (codecName, _)) => setField(udtField, finalFieldName, codecName) }}
                  }

                  override def fromObject(udtValue: UDTValue): $udtType = {
                    ${
                      createExtractMethod(
                        field.typeSignature.typeSymbol.asType,
                        udtCodecs.map { case ((udtField, finalFieldName), (_, _)) => udtField -> finalFieldName },
                        codecNamePrefix = namePrefix + field.name + "_",
                        rowVar = TermName("udtValue")
                      )
                    }
                  }
               }
             }
           """
          // @formatter:on
      }

    }
    // format: ON
  }

  private def getCodecType(field: Symbol, annots: AnnotationsMap): CodecType = {
    def extractCqlType: Option[Type] = {
      annots
        .get(classOf[Column].getName)
        .flatMap(_.get("cqlType"))
        .map(stringToType)
    }

    val cqlType = extractCqlType.getOrElse {
      defaultCqlType {
        val n = field.typeSignature
        if (n.erasure == ScalaTypes.Option) {
          field.typeSignature.typeArgs.head
        } else n
      }
    }

    val typeArgs = cqlType.typeArgs

    cqlType.erasure match {
      case _ if cqlType.typeSymbol == typeOf[CqlType.List[_]].typeSymbol => CodecType.List(inT = typeArgs.head)
      case _ if cqlType.typeSymbol == typeOf[CqlType.Set[_]].typeSymbol => CodecType.Set(inT = typeArgs.head)
      case _ if cqlType.typeSymbol == typeOf[CqlType.Map[_, _]].typeSymbol => CodecType.Map(k = typeArgs.head, v = typeArgs(1))
      case _ if cqlType.typeSymbol == typeOf[CqlType.Tuple2[_, _]].typeSymbol => CodecType.Tuple2(a1 = typeArgs.head, a2 = typeArgs(1))

      case CqlTypes.UDT => createUDTCodec(field)

      case _ =>
        CodecType.Simple(t = field.typeSignature.resultType, ct = cqlType)
    }

  }

  private def getAnnotations(symbol: Symbol): AnnotationsMap = {
    val annotsTypes = symbol.annotations.map(_.tree.tpe.typeSymbol.fullName)
    val annotsParams = symbol.annotations.map {
      _.tree.children.tail.map {
        case q" $name = $value " =>
          name.toString() -> c.eval(c.Expr(q"$value")).toString
      }.toMap
    }

    annotsTypes.zip(annotsParams).toMap
  }

  private def createUDTCodec(field: Symbol): CodecType.UDT = {
    val udtType = field.typeSignature
    val udtSymbol = toCaseClassSymbol(udtType)

    val udtFields = extractFields(udtType)

    val fieldsAndCodecs = udtFields.map {
      case (udtField, (codecType, annots)) => (udtField, fieldFinalName(udtField, annots)) -> codecType
    }

    val udtProps = extractUDTProperties(udtType, udtSymbol)
    val udtName = udtProps.udtName.getOrElse(udtSymbol.name.toString)

    CodecType.UDT(name = udtName, udtFields = fieldsAndCodecs)
  }

  private def defaultCqlType(ts: Type): Type = {
    val typeArgs = ts.typeArgs

    ts.erasure match {
      case ScalaTypes.String => CqlTypes.VarChar
      case ScalaTypes.Int => CqlTypes.Int
      case ScalaTypes.UUID => CqlTypes.UUID
      case ScalaTypes.Boolean => CqlTypes.Boolean
      case ScalaTypes.ByteArray => CqlTypes.Blob
      case ScalaTypes.Double => CqlTypes.Double
      case ScalaTypes.Float => CqlTypes.Float
      case ScalaTypes.LocalDate => CqlTypes.Date
      case ScalaTypes.Instant => CqlTypes.Timestamp

      case ScalaTypes.Set => CqlTypes.Set(typeArgs.head)
      case ScalaTypes.Seq => CqlTypes.List(typeArgs.head)
      case ScalaTypes.SeqMutable => CqlTypes.List(typeArgs.head)
      case ScalaTypes.Map => CqlTypes.Map(typeArgs.head, typeArgs(1))
      case ScalaTypes.Tuple2 => CqlTypes.Tuple2(typeArgs.head, typeArgs(1))
      // TODO other types
      case a =>
        c.abort(
          c.enclosingPosition,
          s"Could not derive default CqlType for Scala type $a, please provide explicit CqlType by @Column annotation for field with type $ts"
        )
    }
  }

  private def javaClassForCqlType(cqlType: Type): Type = {
    val typeArgs = cqlType.typeArgs

    cqlType.erasure match {
      case CqlTypes.VarChar => typeOf[java.lang.String]
      case CqlTypes.Ascii => typeOf[java.lang.String]
      case CqlTypes.Int => typeOf[java.lang.Integer]
      case CqlTypes.UUID => typeOf[java.util.UUID]
      case CqlTypes.TimeUUID => typeOf[java.util.UUID]
      case CqlTypes.Boolean => typeOf[java.lang.Boolean]
      case CqlTypes.Blob => typeOf[ByteBuffer]
      case CqlTypes.Double => typeOf[java.lang.Double]
      case CqlTypes.Float => typeOf[java.lang.Float]
      case CqlTypes.Date => typeOf[Datastax.LocalDate]
      case CqlTypes.Timestamp => typeOf[java.util.Date]
      // TODO other types

      case _ if cqlType.typeSymbol == typeOf[CqlType.Tuple2[_, _]].typeSymbol => typeOf[Datastax.TupleValue]
      case _ if cqlType.typeSymbol == typeOf[CqlType.List[_]].typeSymbol => stringToType(s"java.util.List[${typeArgs.head}]")
      case _ if cqlType.typeSymbol == typeOf[CqlType.Set[_]].typeSymbol => stringToType(s"java.util.Set[${typeArgs.head}]")
      case _ if cqlType.typeSymbol == typeOf[CqlType.Map[_, _]].typeSymbol =>
        val k = typeArgs.head
        val v = typeArgs(1)
        getType(tq"java.util.Map[$k, $v]")
    }
  }

  private def scalaCodecForCqlType(cqlType: Type): Tree = {
    cqlType.erasure match {
      case CqlTypes.VarChar => q"ScalaCodec.varchar"
      case CqlTypes.Ascii => q"ScalaCodec.ascii"
      case CqlTypes.Int => q"ScalaCodec.int"
      case CqlTypes.UUID => q"ScalaCodec.uuid"
      case CqlTypes.TimeUUID => q"ScalaCodec.timeUuid"
      case CqlTypes.Boolean => q"ScalaCodec.boolean"
      case CqlTypes.Blob => q"ScalaCodec.blob"
      case CqlTypes.Double => q"ScalaCodec.double"
      case CqlTypes.Float => q"ScalaCodec.float"
      case CqlTypes.Date => q"ScalaCodec.date"
      case CqlTypes.Timestamp => q"ScalaCodec.timestamp"
      // TODO other types
    }
  }

  private def createTupleType(cqlTypes: Type*): Tree = {
    def toDataType(cqlType: Type): Tree = {
      val typeArgs = cqlType.typeArgs

      cqlType.erasure match {
        case CqlTypes.VarChar => q"Datastax.DataType.varchar()"
        case CqlTypes.Ascii => q"Datastax.DataType.ascii()"
        case CqlTypes.Int => q"Datastax.DataType.cint()"
        case CqlTypes.UUID => q"Datastax.DataType.uuid()"
        case CqlTypes.TimeUUID => q"Datastax.DataType.timeuuid()"
        case CqlTypes.Boolean => q"Datastax.DataType.cboolean()"
        case CqlTypes.Blob => q"Datastax.DataType.blob()"
        case CqlTypes.Double => q"Datastax.DataType.cdouble()"
        case CqlTypes.Float => q"Datastax.DataType.cfloat()"
        case CqlTypes.Date => q"Datastax.DataType.date()"
        case CqlTypes.Timestamp => q"Datastax.DataType.timestamp()"
        // TODO other types

        case _ if cqlType.typeSymbol == typeOf[CqlType.List[_]].typeSymbol => q"Datastax.DataType.list(${toDataType(typeArgs.head)})"
        case _ if cqlType.typeSymbol == typeOf[CqlType.Set[_]].typeSymbol => q"Datastax.DataType.set(${toDataType(typeArgs.head)})"
        case _ if cqlType.typeSymbol == typeOf[CqlType.Map[_, _]].typeSymbol =>
          val k = typeArgs.head
          val v = typeArgs(1)
          q"Datastax.DataType.map(${toDataType(k)}, ${toDataType(v)})"
        case _ if cqlType.typeSymbol == typeOf[CqlType.Tuple2[_, _]].typeSymbol =>
          // TODO support tuples nesting
          val k = typeArgs.head
          val v = typeArgs(1)
          createTupleType(k, v)
      }
    }

    val dataTypes = cqlTypes.map(toDataType)

    q"cassandraInstance.session.getCluster.getMetadata.newTupleType(..$dataTypes)"
  }

  private def getVariable: Tree = {
    val variable = c.prefix.tree match {
      case q"dapper.this.`package`.${_}($n)" => n
      case q"com.avast.dapper.`package`.${_}($n)" => n

      case t => c.abort(c.enclosingPosition, s"Cannot process the conversion - variable name extraction from tree '$t' failed")
    }

    q" $variable "
  }

  private def extractFields(entityType: Type): Map[Symbol, (CodecType, AnnotationsMap)] = {
    val entityCtor = entityType.decls
      .collectFirst {
        case m: MethodSymbol if m.isPrimaryConstructor => m
      }
      .getOrElse(c.abort(c.enclosingPosition, s"Unable to extract ctor from type ${entityType.typeSymbol}"))

    if (entityCtor.paramLists.length != 1) {
      c.abort(c.enclosingPosition, s"Provided type ${entityType.typeSymbol} must have exactly 1 parameter list")
    }

    val fields = entityCtor.paramLists.head
    val withAnnotations = fields zip fields.map(getAnnotations)

    withAnnotations.map {
      case (field, annots) =>
        val codecType = getCodecType(field, annots)

        field -> (codecType, annots)
    }.toMap
  }

  private def toCaseClassSymbol(entityType: Type): ClassSymbol = {
    if (!entityType.typeSymbol.isClass) {
      c.abort(c.enclosingPosition, s"Provided type ${entityType.typeSymbol} is not a class")
    }

    val entitySymbol = entityType.typeSymbol.asClass

    if (!entitySymbol.isCaseClass) {
      c.abort(c.enclosingPosition, s"Provided type ${entityType.typeSymbol} is not a case class")
    }
    entitySymbol
  }

  private def stringToType(s: String): Type = {
    c.typecheck(c.parse(s"???.asInstanceOf[$s]")).tpe
  }

  def getType(typeTree: Tree): Type = c.typecheck(typeTree, c.TYPEmode).tpe

  private sealed trait CodecType

  private object CodecType {

    case class Simple(t: Type, ct: Type) extends CodecType

    case class List(inT: Type) extends CodecType

    case class Set(inT: Type) extends CodecType

    case class Map(k: Type, v: Type) extends CodecType

    case class Tuple2(a1: Type, a2: Type) extends CodecType

    case class UDT(name: String, udtFields: scala.Predef.Map[(Symbol, String), CodecType]) extends CodecType

  }

}

object Macros {

  private type AnnotationsMap = Map[String, Map[String, String]]

}
