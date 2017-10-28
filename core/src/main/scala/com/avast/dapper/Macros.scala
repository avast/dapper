package com.avast.dapper

import java.nio.ByteBuffer

import com.avast.dapper.Macros.AnnotationsMap
import com.avast.dapper.dao.{CassandraDao, CassandraEntity, Column, CqlType, PartitionKey, ScalaCodec, Table}

import scala.reflect.ClassTag
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

    final val List = typeOf[CqlType.List[_]]
    final val Set = typeOf[CqlType.Set[_]]
//    final def Map[K <: CqlType, V <: CqlType]: c.universe.TypeSymbol = typeOf[CqlType.Map[K, V]].typeSymbol.asType
//    final def Tuple2[A1 <: CqlType, A2 <: CqlType]: c.universe.TypeSymbol = typeOf[CqlType.Tuple2[A1, A2]].typeSymbol.asType

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
  }

  // format: OFF
  def createDao[PrimaryKey: c.WeakTypeTag, Entity <: CassandraEntity[PrimaryKey] : c.WeakTypeTag]: c.Expr[CassandraDao[PrimaryKey, Entity]] = {
    // format: ON

    val primaryKeyType = weakTypeOf[PrimaryKey]
    val entityType = weakTypeOf[Entity]

    if (!entityType.typeSymbol.isClass) {
      c.abort(c.enclosingPosition, s"Provided type ${entityType.typeSymbol} is not a class")
    }

    val entitySymbol = entityType.typeSymbol.asClass

    if (!entitySymbol.isCaseClass) {
      c.abort(c.enclosingPosition, s"Provided type ${entityType.typeSymbol} is not a case class")
    }

    entitySymbol.annotations.collectFirst {
      case a if a.toString == classOf[Table].getName =>
    }

    val entityCtor = entityType.decls
      .collectFirst {
        case m: MethodSymbol if m.isPrimaryConstructor => m
      }
      .getOrElse(c.abort(c.enclosingPosition, s"Unable to extract ctor from type ${entityType.typeSymbol}"))

    if (entityCtor.paramLists.length != 1) {
      c.abort(c.enclosingPosition, s"Provided type ${entityType.typeSymbol} must have exactly 1 parameter list")
    }

    val entityFields: Map[Symbol, (CodecType, AnnotationsMap)] = {

      val fields = entityCtor.paramLists.head
      val withAnnotations = fields zip fields.map(getAnnotations)

      withAnnotations.map {
        case (field, annots) =>
          val codecType = getCodecType(field, annots)

          field -> (codecType, annots)
      }
    }.toMap

    val primaryKeyFields = entityFields
      .collect {
        case (field, (cqlType, annots)) if annots contains classOf[PartitionKey].getName =>
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

    val codecs: Iterable[Tree] = entityFields.zipWithIndex.map {
      case ((field, (codecType, annots)), index) =>
        codec(index, field, codecType)
    }

    val m =
      q"""
      new EntityMapper[$primaryKeyType, $entityType] {

        private val cassandraInstance = $getVariable

        ..$codecs

        cassandraInstance.codecRegistry.register(..${entityFields.map(f => q"${TermName("codec_" + f._1.name)}.javaTypeCodec")})

        override def primaryKeyPattern: String = ${primaryKeyFields.map(_.name + " = ?").mkString(" and ")}

        override def getPrimaryKey(instance: $entityType): $primaryKeyType = (..${primaryKeyFields.map(s => q"instance.$s")})

        override def convertPrimaryKey(k: $primaryKeyType): Seq[Object] = ${convertPrimaryKey(primaryKeyFields)}

        override def extract(r: ResultSet): $entityType = ???

        override def save(tableName: String, e: $entityType): Statement = ???
      }
      """

    c.abort(c.enclosingPosition, m.toString())
  }

  private def convertPrimaryKey(primaryKeyFields: Seq[c.universe.Symbol]): Tree = {
    val withIndex = primaryKeyFields.zipWithIndex

    val mappings = withIndex.map {
      case (field, index) =>
        q"${TermName("codec_" + field.name)}.toObject(${TermName("k._" + (index + 1))})"
    }

    q""" Seq(..$mappings) """
  }

  private def codec(index: Int, field: c.universe.Symbol, codecType: CodecType): c.universe.Tree = {
    val c = codecType match {
      case CodecType.Simple(t, ct) => q"implicitly[ScalaCodec[$t, ${javaClassForCqlType(ct)}, $ct]]"
      case CodecType.List(inT) => q"ScalaCodec.list(${scalaCodecForCqlType(inT)})"
    }

    q""" private val ${TermName("codec_" + field.name)} = $c """
  }

  private def getCodecType(field: Symbol, annots: AnnotationsMap): CodecType = {
    def extractCqlType: Option[Type] = {
      annots
        .get(classOf[Column].getName)
        .flatMap(_.get("cqlType"))
        .map(stringToType)
    }

    val cqlType = extractCqlType.getOrElse(defaultCqlType(field))

    cqlType match {
      case ct if ct.typeSymbol == CqlTypes.List.typeSymbol =>
        CodecType.List(inT = ct.typeArgs.head.erasure)

      case ct =>
        CodecType.Simple(t = field.typeSignature.resultType, ct = ct)
    }

  }

  private def getAnnotations(field: c.universe.Symbol): AnnotationsMap = {
    val annotsTypes = field.annotations.map(_.tree.tpe.typeSymbol.fullName)
    val annotsParams = field.annotations.map {
      _.tree.children.tail.map {
        case q" $name = $value " =>
          name.toString() -> c.eval(c.Expr(q"$value")).toString
      }.toMap
    }

    annotsTypes.zip(annotsParams).toMap
  }

  private def defaultCqlType(field: c.universe.Symbol): Type = {
    import c.universe._

    val ts: Type = {
      val n = field.typeSignature.erasure
      if (n == ScalaTypes.Option) {
        field.typeSignature.typeArgs.head.erasure
      } else n
    }

    ts match {
      case ScalaTypes.String => CqlTypes.VarChar
      case ScalaTypes.Int => CqlTypes.Int
      case ScalaTypes.UUID => CqlTypes.UUID
      case ScalaTypes.Boolean => CqlTypes.Boolean
      case ScalaTypes.ByteArray => CqlTypes.Blob
      case ScalaTypes.Double => CqlTypes.Double
      case ScalaTypes.Float => CqlTypes.Float
      case ScalaTypes.LocalDate => CqlTypes.Date
      case ScalaTypes.Instant => CqlTypes.Timestamp
      // TODO other types
      case a =>
        c.abort(
          c.enclosingPosition,
          s"Could not derive default CqlType for Scala type $a, please provide explicit CqlType by @Column annotation for field '${field.name}'"
        )
    }
  }

  private def javaClassForCqlType(cqlType: Type): Type = {
    cqlType match {
      case CqlTypes.VarChar => typeOf[java.lang.String]
      case CqlTypes.Ascii => typeOf[java.lang.String]
      case CqlTypes.Int => typeOf[java.lang.Integer]
      case CqlTypes.UUID => typeOf[java.util.UUID]
      case CqlTypes.TimeUUID => typeOf[java.util.UUID]
      case CqlTypes.Boolean => typeOf[java.lang.Boolean]
      case CqlTypes.Blob => typeOf[ByteBuffer]
      case CqlTypes.Double => typeOf[java.lang.Double]
      case CqlTypes.Float => typeOf[java.lang.Float]
      case CqlTypes.Date => typeOf[com.datastax.driver.core.LocalDate]
      case CqlTypes.Timestamp => typeOf[java.util.Date]
      // TODO other types

      case ct if ct.typeSymbol == CqlTypes.List.typeSymbol =>
        val typeArg = ct.typeArgs.head
        stringToType(s"java.util.List[$typeArg]")
    }
  }

  private def scalaCodecForCqlType(cqlType: Type): Tree = {
    cqlType match {
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

  private def getVariable: Tree = {
    // TODO improve
    val variable = c.prefix.tree match {
      case q"dapper.this.`package`.${_}[${_}]($n)" => n
      case q"dapper.this.`package`.${_}($n)" => n
      case q"com.avast.dapper.`package`.${_}[${_}]($n)" => n
      case q"com.avast.dapper.`package`.${_}($n)" => n

      case n @ q"new com.avast.dapper.Cassandra(..${_})" => n

      case t => c.abort(c.enclosingPosition, s"Cannot process the conversion - variable name extraction from tree '$t' failed")
    }

    q" $variable "
  }

  private def extractType[A: ClassTag]: c.universe.Type = {
    stringToType(implicitly[ClassTag[A]].runtimeClass.getName)
  }

  private def stringToType(s: String): c.universe.Type = {
    c.typecheck(c.parse(s"???.asInstanceOf[$s]")).tpe
  }

  private sealed trait CodecType

  private object CodecType {

    case class Simple(t: Type, ct: Type) extends CodecType

    case class List(inT: Type) extends CodecType

    //
    //    case class Set(t: TypeSymbol, ct: TypeSymbol) extends CodecType
    //
    //    case class Map(kT: TypeSymbol, kCt: TypeSymbol, vT: TypeSymbol, vCt: TypeSymbol) extends CodecType

  }

}

object Macros {

  private type AnnotationsMap = Map[String, Map[String, String]]

}
