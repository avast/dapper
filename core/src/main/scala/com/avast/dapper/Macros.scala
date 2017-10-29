package com.avast.dapper

import java.nio.ByteBuffer

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

    val entitySymbol = toCaseClassSymbol(entityType)

    val tableName = getAnnotations(entitySymbol)
      .collectFirst {
        case (n, params) if n == classOf[Table].getName => params.get("name")
      }
      .flatten
      .getOrElse {
        c.abort(c.enclosingPosition, s"Provided type ${entityType.typeSymbol} must have at least one PartitionKey annotated field")
      }

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

      implicit val mapper = new EntityMapper[$primaryKeyType, $entityType] {

        ..${codecs.values}

        cassandraInstance.codecRegistry.register(..${codecs.keys.map(n => q"${TermName("codec_" + n)}.javaTypeCodec")})

        override def primaryKeyPattern: String = ${primaryKeyFields.map(_.name + " = ?").mkString(" and ")}

        override def getPrimaryKey(instance: $entityType): $primaryKeyType = (..${primaryKeyFields.map(s =>
        q"instance.${TermName(s.name.toString)}")})

        override def convertPrimaryKey(k: $primaryKeyType): Seq[Object] = ${convertPrimaryKey(primaryKeyFields)}

        override def extract(r: ResultSet): $entityType = ???

        override def save(tableName: String, e: $entityType): Statement = ${createSaveMethod(tableName, entityFields)}
      }
      """

    val dao =
      q"""
         {
            val cassandraInstance = $getVariable

            $mapper

            new CassandraDao[$primaryKeyType, $entityType]($tableName, cassandraInstance.session)
         }
       """

    println(dao)

    //    c.Expr[CassandraDao[PrimaryKey, Entity]](dao)
    c.abort(c.enclosingPosition, dao.toString())
  }

  private def convertPrimaryKey(primaryKeyFields: Seq[Symbol]): Tree = {
    val withIndex = primaryKeyFields.zipWithIndex

    val mappings = withIndex.map {
      case (field, index) =>
        q"${TermName("codec_" + field.name)}.toObject(${TermName("_" + (index + 1))})" // for some unknown reason "k._1" cannot be used :-(
    }

    q""" { import k._; Seq(..$mappings) } """
  }

  private def createSaveMethod(tableName: String, entityFields: Map[Symbol, (CodecType, AnnotationsMap)]): Tree = {
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

    val m = entityFields.toSeq.map {
      case (field, (codecType, _)) =>
        val placeHolder = fieldToPlaceholder(field, codecType)
        val bindings = fieldToBindings(field, codecType)

        placeHolder -> bindings
    }

    val placeHolders = m.map(_._1)
    val bindings = m.flatMap(_._2)

    // TODO support customized name
    val query = s"insert into $tableName (${entityFields.map(_._1.name).mkString(", ")}) values (${placeHolders.mkString(", ")})"

    q"""
       new SimpleStatement(
          $query,
          ..$bindings
       )
     """
  }

  private def codec(field: c.universe.Symbol, codecType: CodecType, namePrefix: String = ""): Map[String, Tree] = {
    def wrapWithVal(name: String, codec: Tree): Map[String, Tree] = {
      Map(name -> q""" private val ${TermName("codec_" + name)} = $codec """)
    }

    codecType match {
      case CodecType.Simple(t, ct) => wrapWithVal(namePrefix + field.name, q"implicitly[ScalaCodec[$t, ${javaClassForCqlType(ct)}, $ct]]")
      case CodecType.List(inT) => wrapWithVal(namePrefix + field.name.toString, q"ScalaCodec.list(${scalaCodecForCqlType(inT)})")
      case CodecType.UDT(codecs) =>
        codecs.flatMap {
          case (udtField, udtCodec) => codec(udtField, udtCodec, namePrefix + field.name + "_")
        }
    }
  }

  private def getCodecType(field: Symbol, annots: AnnotationsMap): CodecType = {
    def extractCqlType: Option[Type] = {
      annots
        .get(classOf[Column].getName)
        .flatMap(_.get("cqlType"))
        .map(stringToType)
    }

    val cqlType = extractCqlType.getOrElse(defaultCqlType(field))

    cqlType.erasure match {
      case _ if cqlType.typeSymbol == CqlTypes.List.typeSymbol =>
        CodecType.List(inT = cqlType.typeArgs.head.erasure)

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
      case CqlTypes.Date => typeOf[Datastax.LocalDate]
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

  private def extractFields(entityType: c.universe.Type): Map[c.universe.Symbol, (CodecType, AnnotationsMap)] = {
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

  private def toCaseClassSymbol(entityType: c.universe.Type): ClassSymbol = {
    if (!entityType.typeSymbol.isClass) {
      c.abort(c.enclosingPosition, s"Provided type ${entityType.typeSymbol} is not a class")
    }

    val entitySymbol = entityType.typeSymbol.asClass

    if (!entitySymbol.isCaseClass) {
      c.abort(c.enclosingPosition, s"Provided type ${entityType.typeSymbol} is not a case class")
    }
    entitySymbol
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

    case class UDT(codecs: Map[Symbol, CodecType]) extends CodecType

  }

}

object Macros {

  private type AnnotationsMap = Map[String, Map[String, String]]

}
