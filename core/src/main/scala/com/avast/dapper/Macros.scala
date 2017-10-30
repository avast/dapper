package com.avast.dapper

import java.nio.ByteBuffer
import java.time.{Duration, Instant}

import com.avast.dapper.Macros.AnnotationsMap
import com.avast.dapper.dao.{CassandraDao, CassandraEntity, Column, CqlType, PartitionKey, Table}
import com.datastax.driver.{core => Datastax}

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
  def createDao[PrimaryKey: c.WeakTypeTag, Entity <: CassandraEntity[PrimaryKey] : c.WeakTypeTag]: c.Expr[CassandraDao[PrimaryKey, Entity]] = {
    // format: ON

    val primaryKeyType = weakTypeOf[PrimaryKey]
    val entityType = weakTypeOf[Entity]

    val entitySymbol = toCaseClassSymbol(entityType)

    val tableProperties = extractTableProperties(entityType, entitySymbol)
    import tableProperties._

    val entityFields: Map[c.universe.Symbol, (CodecType, AnnotationsMap)] = extractFields(entityType)

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

    val codecs: Map[String, Tree] = entityFields.flatMap {
      case (field, (codecType, _)) =>
        codec(field, codecType)
    }

    val mapper =
      q"""

      private implicit val mapper: EntityMapper[$primaryKeyType, $entityType] = new EntityMapper[$primaryKeyType, $entityType] {

        import com.datastax.driver.{core => Datastax}

        ..${codecs.values}

        val tableName: String = $tableName

        val defaultReadConsistencyLevel: Option[Datastax.ConsistencyLevel] = $defaultReadConsistencyLevel

        val defaultWriteConsistencyLevel: Option[Datastax.ConsistencyLevel] = $defaultWriteConsistencyLevel

        val primaryKeyPattern: String = ${primaryKeyFields.map(_.name + " = ?").mkString(" and ")}

        def getPrimaryKey(instance: $entityType): $primaryKeyType = (..${primaryKeyFields.map(s => q"instance.${TermName(s.name.toString)}")})

        def convertPrimaryKey(k: $primaryKeyType): Seq[Object] = ${convertPrimaryKey(primaryKeyFields)}

        def extract(row: Datastax.Row): $entityType = ${createExtractMethod(entitySymbol, entityFields)}

        def save(tableName: String, e: $entityType, writeOptions: WriteOptions): Statement = ${createSaveMethod(tableName,
                                                                                                                entityFields,
                                                                                                                q"writeOptions")}
      }
      """

    val dao =
      q"""
         {
            val cassandraInstance = $getVariable

            $mapper

            new CassandraDao[$primaryKeyType, $entityType](cassandraInstance.session)
         }
       """

    println(dao)

        c.Expr[CassandraDao[PrimaryKey, Entity]](dao)
//    c.abort(c.enclosingPosition, dao.toString())
  }

  private def extractTableProperties(entityType: Type, entitySymbol: ClassSymbol) = new {
    private val annots = getAnnotations(entitySymbol)

    private val tableAnnot: Map[String, String] = annots
      .collectFirst { case (n, params) if n == classOf[Table].getName => params }
      .getOrElse {
        c.abort(c.enclosingPosition, s"Provided type ${entityType.typeSymbol} must be annotated with @Table")
      }

    // format: OFF
    val tableName: String = tableAnnot.get("name").map {
      // prepend keyspace?
      tableAnnot.get("keyspace").map(_ + ".").getOrElse("") + _
    }.getOrElse {
      c.abort(c.enclosingPosition, s"Provided type ${entityType.typeSymbol} @Table annotation is missing 'name' param")
    }

    val defaultReadConsistencyLevel: Tree = {
      tableAnnot.get("readConsistency").flatMap(_.split(" ").tail.headOption).map(c => q"Some(Datastax.ConsistencyLevel.valueOf($c))").getOrElse(q"None")
    }

    val defaultWriteConsistencyLevel: Tree = {
      tableAnnot.get("writeConsistency").flatMap(_.split(" ").tail.headOption).map(c => q"Some(Datastax.ConsistencyLevel.valueOf($c))").getOrElse(q"None")
    }
    // format: ON

  }

  private def convertPrimaryKey(primaryKeyFields: Seq[Symbol]): Tree = {
    val withIndex = primaryKeyFields.zipWithIndex

    val mappings = withIndex.map {
      case (field, index) =>
        q"${TermName("codec_" + field.name)}.toObject(${TermName("_" + (index + 1))})" // for some unknown reason "k._1" cannot be used :-(
    }

    q""" { import k._; Seq(..$mappings) } """
  }

  private def createSaveMethod(tableName: String, entityFields: Map[Symbol, (CodecType, AnnotationsMap)], writeOptions: Tree): Tree = {
    def fieldToPlaceholder(field: Symbol, codecType: CodecType): String = codecType match {
      case CodecType.UDT(codecs) => codecs.map { case (udtField, _) => s"${udtField.name}: ?" }.mkString("{", ", ", "}")
      case _ => "?"
    }

    def fieldToBindings(field: Symbol, codecType: CodecType): Seq[Tree] = codecType match {
      case CodecType.UDT(codecs) =>
        codecs.map {
          case (udtField, _) =>
            q"${TermName("codec_" + field.name + "_" + udtField.name)}.toObject(e.${TermName(field.name.toString)}.${TermName(udtField.name.toString)})"
        }.toSeq

      case _ => Seq(q"${TermName("codec_" + field.name)}.toObject(e.${TermName(field.name.toString)})")
    }

    def decorateWithOptions(query: String): Tree = {
      q"""
         {
            val usings = {
              val opts = Seq(
                $writeOptions.ttl.map(_.getSeconds).map(" TTL " + _),
                $writeOptions.timestamp.map(_.toEpochMilli * 1000).map(" TIMESTAMP " + _)
              ).flatten

              if (opts.nonEmpty) opts.mkString(" USING ", " AND ", "") else ""
            }

            val ifNotExist = if ($writeOptions.ifNotExist) " IF NOT EXIST" else ""

            $query + ifNotExist + usings
         }
       """
    }

    val m = entityFields.toSeq.map {
      case (field, (codecType, _)) =>
        val placeHolder = fieldToPlaceholder(field, codecType)
        val bindings = fieldToBindings(field, codecType)

        placeHolder -> bindings
    }

    val placeHolders = m.map(_._1)
    val bindings = m.flatMap(_._2)

    // TODO support customized name
    val query = decorateWithOptions {
      s"insert into $tableName (${entityFields.map(_._1.name).mkString(", ")}) values (${placeHolders.mkString(", ")})"
    }

    q"""
       {
          val st = new SimpleStatement(
            $query,
            ..$bindings
          )

          $writeOptions.consistencyLevel.orElse(defaultWriteConsistencyLevel).foreach(st.setConsistencyLevel)

          st
       }
     """
  }

  private def createExtractMethod(entitySymbol: TypeSymbol,
                                  entityFields: Map[Symbol, (CodecType, AnnotationsMap)],
                                  codecNamePrefix: String = "",
                                  rowVar: TermName = TermName("row")): Tree = {

    // TODO support customized name
    val fields: Iterable[Tree] = entityFields.map {
      case (field, (CodecType.UDT(udtCodecs), _)) =>
        val udtTypeSymbol = field.typeSignature.typeSymbol.asType
        val udtFields = udtCodecs.map { case (udtField, codec) => udtField -> (codec, getAnnotations(udtField)) }

        val fieldName = field.name.toString
        val udtRowVar = TermName("row_" + fieldName)
        q"""
            ${TermName(fieldName)} = {
              val $udtRowVar = $rowVar.getUDTValue($fieldName)
              ${createExtractMethod(udtTypeSymbol, udtFields, codecNamePrefix = fieldName + "_", rowVar = udtRowVar)}
            }
          """

      case (field, (_, _)) =>
        val fieldName = field.name.toString
        val codecName = TermName("codec_" + codecNamePrefix + fieldName)

        q"${TermName(fieldName)} = $codecName.fromObject($rowVar.get($fieldName, $codecName.javaTypeCodec))"
    }

    q"""
          new $entitySymbol(
            ..$fields
          )
     """
  }

  private def codec(field: Symbol, codecType: CodecType, namePrefix: String = ""): Map[String, Tree] = {
    def wrapWithVal(name: String, codec: Tree): Map[String, Tree] = {
      Map(name -> q""" private val ${TermName("codec_" + name)} = $codec """)
    }

    // format: OFF
    codecType match {
      case CodecType.Simple(t, ct) => wrapWithVal(namePrefix + field.name, q"implicitly[ScalaCodec[$t, ${javaClassForCqlType(ct)}, $ct]]")
      case CodecType.List(inT) => wrapWithVal(namePrefix + field.name.toString, q"ScalaCodec.list(${scalaCodecForCqlType(inT)})")
      case CodecType.Set(inT) => wrapWithVal(namePrefix + field.name.toString, q"ScalaCodec.set(${scalaCodecForCqlType(inT)})")
      case CodecType.Map(k, v) => wrapWithVal(namePrefix + field.name.toString, q"ScalaCodec.map(${scalaCodecForCqlType(k)}, ${scalaCodecForCqlType(v)})")
      case CodecType.Tuple2(a1, a2) => wrapWithVal(namePrefix + field.name.toString, q"ScalaCodec.tuple2(${createTupleType(a1, a2)})(${scalaCodecForCqlType(a1)}, ${scalaCodecForCqlType(a2)})")
      case CodecType.UDT(codecs) => codecs.flatMap { case (udtField, udtCodec) => codec(udtField, udtCodec, namePrefix + field.name + "_") }
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

  private def getAnnotations(symbol: c.universe.Symbol): AnnotationsMap = {
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

    toCaseClassSymbol(udtType) // don't need result, just check it's a case class

    val udtFields = extractFields(udtType)

    val codecs = udtFields.map {
      case (f, (codecType, _)) => f -> codecType
    }

    CodecType.UDT(codecs = codecs)
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
    //cassandra.underlying.getCluster.getMetadata.newTupleType(types: _*)

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
    q" ${c.prefix.tree} "
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

    case class UDT(codecs: scala.Predef.Map[Symbol, CodecType]) extends CodecType

  }

}

object Macros {

  private type AnnotationsMap = Map[String, Map[String, String]]

}
