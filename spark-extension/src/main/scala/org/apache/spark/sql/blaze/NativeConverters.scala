package org.apache.spark.sql.blaze

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.expressions.{
  Abs,
  Acos,
  Add,
  And,
  Asin,
  Atan,
  AttributeReference,
  CaseWhen,
  Cast,
  Ceil,
  Coalesce,
  Concat,
  Cos,
  DatePart,
  Divide,
  EqualTo,
  Exp,
  Expression,
  Floor,
  GreaterThan,
  GreaterThanOrEqual,
  In,
  InSet,
  IsNotNull,
  IsNull,
  LessThan,
  LessThanOrEqual,
  Like,
  Literal,
  Log,
  Log10,
  Log2,
  Lower,
  Md5,
  Multiply,
  Not,
  NullIf,
  OctetLength,
  Or,
  Remainder,
  Round,
  Sha2,
  Signum,
  Sin,
  Sqrt,
  StartsWith,
  StringTrim,
  StringTrimLeft,
  StringTrimRight,
  Substring,
  Subtract,
  Tan,
  TruncDate,
  Upper
}
import org.apache.spark.sql.catalyst.plans.FullOuter
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.LeftAnti
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.catalyst.plans.LeftSemi
import org.apache.spark.sql.catalyst.plans.RightOuter
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.NullType
import org.apache.spark.sql.types.ShortType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import org.blaze.protobuf.ArrowType
import org.blaze.protobuf.BinaryExprNode
import org.blaze.protobuf.CaseNode
import org.apache.spark.unsafe.types.UTF8String
import org.blaze.protobuf.CastNode
import org.blaze.protobuf.Column
import org.blaze.protobuf.EmptyMessage
import org.blaze.protobuf.Field
import org.blaze.protobuf.InListNode
import org.blaze.protobuf.LogicalExprNode
import org.blaze.protobuf.PhysicalBinaryExprNode
import org.blaze.protobuf.PhysicalCaseNode
import org.blaze.protobuf.PhysicalCastNode
import org.blaze.protobuf.PhysicalColumn
import org.blaze.protobuf.PhysicalExprNode
import org.blaze.protobuf.PhysicalInListNode
import org.blaze.protobuf.PhysicalIsNotNull
import org.blaze.protobuf.PhysicalNot
import org.blaze.protobuf.PhysicalScalarFunctionNode
import org.blaze.protobuf.PhysicalWhenThen
import org.blaze.protobuf.PrimitiveScalarType
import org.blaze.protobuf.ScalarDecimalValue
import org.blaze.protobuf.ScalarFunction
import org.blaze.protobuf.ScalarFunctionNode
import org.blaze.protobuf.ScalarValue
import org.blaze.protobuf.Schema
import org.blaze.protobuf.Timestamp
import org.blaze.protobuf.WhenThen
import org.blaze.protobuf.PhysicalIsNull

object NativeConverters {
  def convertToScalarType(dt: DataType): PrimitiveScalarType = {
    dt match {
      case NullType => PrimitiveScalarType.NULL
      case BooleanType => PrimitiveScalarType.BOOL
      case ByteType => PrimitiveScalarType.INT8
      case ShortType => PrimitiveScalarType.INT16
      case IntegerType => PrimitiveScalarType.INT32
      case LongType => PrimitiveScalarType.INT64
      case FloatType => PrimitiveScalarType.FLOAT32
      case DoubleType => PrimitiveScalarType.FLOAT64
      case StringType => PrimitiveScalarType.UTF8
      case _ => throw new NotImplementedError(s"convert $dt to DF scalar type not supported")
    }
  }

  def convertDataType(sparkDataType: DataType): ArrowType = {
    val arrowTypeBuilder = ArrowType.newBuilder()
    sparkDataType match {
      case NullType => arrowTypeBuilder.setNONE(EmptyMessage.getDefaultInstance)
      case BooleanType => arrowTypeBuilder.setBOOL(EmptyMessage.getDefaultInstance)
      case ByteType => arrowTypeBuilder.setINT8(EmptyMessage.getDefaultInstance)
      case ShortType => arrowTypeBuilder.setINT16(EmptyMessage.getDefaultInstance)
      case IntegerType => arrowTypeBuilder.setINT32(EmptyMessage.getDefaultInstance)
      case LongType => arrowTypeBuilder.setINT64(EmptyMessage.getDefaultInstance)
      case FloatType => arrowTypeBuilder.setFLOAT32(EmptyMessage.getDefaultInstance)
      case DoubleType => arrowTypeBuilder.setFLOAT64(EmptyMessage.getDefaultInstance)
      case StringType => arrowTypeBuilder.setUTF8(EmptyMessage.getDefaultInstance)
      case BinaryType => arrowTypeBuilder.setBINARY(EmptyMessage.getDefaultInstance)
      case DateType => arrowTypeBuilder.setDATE32(EmptyMessage.getDefaultInstance)
      case TimestampType =>
        arrowTypeBuilder.setTIMESTAMP(Timestamp.getDefaultInstance) // NOTE: microsecond => millisecond

      // decimal
      case t: DecimalType =>
        arrowTypeBuilder.setDECIMAL(
          org.blaze.protobuf.Decimal
            .newBuilder()
            .setWhole(t.precision) // precision - scale?
            .setFractional(t.scale)
            .build())

      // TODO: support complex data types
      case _ =>
        throw new NotImplementedError(s"Data type conversion not implemented ${sparkDataType}")
    }
    arrowTypeBuilder.build()
  }

  def convertValue(sparkValue: Any, dataType: DataType): ScalarValue = {
    val scalarValueBuilder = ScalarValue.newBuilder()
    dataType match {
      case NullType => scalarValueBuilder.setNullValue(PrimitiveScalarType.NULL)
      case BooleanType => scalarValueBuilder.setBoolValue(sparkValue.asInstanceOf[Boolean])
      case ByteType => scalarValueBuilder.setInt8Value(sparkValue.asInstanceOf[Byte])
      case ShortType => scalarValueBuilder.setInt16Value(sparkValue.asInstanceOf[Short])
      case IntegerType => scalarValueBuilder.setInt32Value(sparkValue.asInstanceOf[Int])
      case LongType => scalarValueBuilder.setInt64Value(sparkValue.asInstanceOf[Long])
      case FloatType => scalarValueBuilder.setFloat32Value(sparkValue.asInstanceOf[Float])
      case DoubleType => scalarValueBuilder.setFloat64Value(sparkValue.asInstanceOf[Double])
      case StringType => scalarValueBuilder.setUtf8Value(sparkValue.toString)
      case BinaryType => throw new NotImplementedError("???")
      case DateType => scalarValueBuilder.setDate32Value(sparkValue.asInstanceOf[Int])
      case TimestampType =>
        scalarValueBuilder.setTimeMicrosecondValue(sparkValue.asInstanceOf[Long])
      case t: DecimalType => {
        val decimalValue = sparkValue.asInstanceOf[Decimal]
        val decimalType = convertDataType(t).getDECIMAL
        scalarValueBuilder.setDecimalValue(
          ScalarDecimalValue
            .newBuilder()
            .setDecimal(decimalType)
            .setLongValue(decimalValue.toUnscaledLong))
      }
      // TODO: support complex data types
      case _ => throw new NotImplementedError(s"Value conversion not implemented ${dataType}")
    }

    // TODO: support complex data types
    scalarValueBuilder.build()
  }

  def convertField(sparkField: StructField): Field = {
    Field
      .newBuilder()
      .setName(sparkField.name)
      .setNullable(sparkField.nullable)
      .setArrowType(convertDataType(sparkField.dataType))
      .build()
  }

  def convertSchema(sparkSchema: StructType): Schema = {
    val schemaBuilder = Schema.newBuilder()
    sparkSchema.foreach(sparkField => schemaBuilder.addColumns(convertField(sparkField)))
    schemaBuilder.build()
  }

  def convertExpr(sparkExpr: Expression): PhysicalExprNode = {
    def buildExprNode(
        buildFn: (PhysicalExprNode.Builder) => PhysicalExprNode.Builder): PhysicalExprNode =
      buildFn(PhysicalExprNode.newBuilder()).build()

    def buildBinaryExprNode(left: Expression, right: Expression, op: String): PhysicalExprNode =
      buildExprNode {
        _.setBinaryExpr(
          PhysicalBinaryExprNode
            .newBuilder()
            .setL(convertExpr(left))
            .setR(convertExpr(right))
            .setOp(op)
            .build())
      }

    def buildScalarFunction(
        fn: ScalarFunction,
        args: Seq[Expression],
        dataType: DataType): PhysicalExprNode = buildExprNode {
      _.setScalarFunction(
        PhysicalScalarFunctionNode
          .newBuilder()
          .setName(fn.name())
          .setFun(fn)
          .addAllArgs(args.map(convertExpr).asJava)
          .setReturnType(convertDataType(dataType))
          .build())
    }

    def unpackBinaryTypeCast(expr: Expression) = expr match {
      case Cast(inner, BinaryType, _) => inner
      case expr => expr
    }

    sparkExpr match {
      case l @ Literal(value, dataType) =>
        buildExprNode { b =>
          if (!l.nullable) {
            b.setLiteral(convertValue(value, dataType))
          } else {
            b.setLiteral(ScalarValue.newBuilder().setNullValue(convertToScalarType(dataType)))
          }
        }
      case AttributeReference(name, _, _, _) =>
        buildExprNode {
          _.setColumn(PhysicalColumn.newBuilder().setName(name).build())
        }

      // cast
      case Cast(child, dataType, _) =>
        buildExprNode {
          _.setCast(
            PhysicalCastNode
              .newBuilder()
              .setExpr(convertExpr(child))
              .setArrowType(convertDataType(dataType))
              .build())
        }

      // in
      case In(value, list) =>
        buildExprNode {
          _.setInList(
            PhysicalInListNode
              .newBuilder()
              .setExpr(convertExpr(value))
              .addAllList(list.map(convertExpr).asJava))
        }

      // in
      case InSet(value, set) =>
        buildExprNode {
          _.setInList(
            PhysicalInListNode
              .newBuilder()
              .setExpr(convertExpr(value))
              .addAllList(set.map {
                case utf8string: UTF8String => convertExpr(Literal(utf8string, StringType))
                case v => convertExpr(Literal.apply(v))
              }.asJava))
        }

      // unary ops
      case IsNull(child) =>
        buildExprNode {
          _.setIsNullExpr(PhysicalIsNull.newBuilder().setExpr(convertExpr(child)).build())
        }
      case IsNotNull(child) =>
        buildExprNode {
          _.setIsNotNullExpr(PhysicalIsNotNull.newBuilder().setExpr(convertExpr(child)).build())
        }
      case Not(child) =>
        buildExprNode {
          _.setNotExpr(PhysicalNot.newBuilder().setExpr(convertExpr(child)).build())
        }

      // binary ops
      case EqualTo(lhs, rhs) => buildBinaryExprNode(lhs, rhs, "Eq")
      case GreaterThan(lhs, rhs) => buildBinaryExprNode(lhs, rhs, "Gt")
      case LessThan(lhs, rhs) => buildBinaryExprNode(lhs, rhs, "Lt")
      case GreaterThanOrEqual(lhs, rhs) => buildBinaryExprNode(lhs, rhs, "GtEq")
      case LessThanOrEqual(lhs, rhs) => buildBinaryExprNode(lhs, rhs, "LtEq")
      case Add(lhs, rhs) => buildBinaryExprNode(lhs, rhs, "Plus")
      case Subtract(lhs, rhs) => buildBinaryExprNode(lhs, rhs, "Minus")
      case Multiply(lhs, rhs) => buildBinaryExprNode(lhs, rhs, "Multiply")
      case Divide(lhs, rhs) => buildBinaryExprNode(lhs, rhs, "Divide")
      case Remainder(lhs, rhs) => buildBinaryExprNode(lhs, rhs, "Modulo")
      case Like(lhs, rhs, '\\') => buildBinaryExprNode(lhs, rhs, "Like")
      case And(lhs, rhs) => buildBinaryExprNode(lhs, rhs, "And")
      case Or(lhs, rhs) => buildBinaryExprNode(lhs, rhs, "Or")

      // builtin scalar functions
      case e: Sqrt => buildScalarFunction(ScalarFunction.Sqrt, e.children, e.dataType)
      case e: Sin => buildScalarFunction(ScalarFunction.Sin, e.children, e.dataType)
      case e: Cos => buildScalarFunction(ScalarFunction.Cos, e.children, e.dataType)
      case e: Tan => buildScalarFunction(ScalarFunction.Tan, e.children, e.dataType)
      case e: Asin => buildScalarFunction(ScalarFunction.Asin, e.children, e.dataType)
      case e: Acos => buildScalarFunction(ScalarFunction.Acos, e.children, e.dataType)
      case e: Atan => buildScalarFunction(ScalarFunction.Atan, e.children, e.dataType)
      case e: Exp => buildScalarFunction(ScalarFunction.Exp, e.children, e.dataType)
      case e: Log => buildScalarFunction(ScalarFunction.Log, e.children, e.dataType)
      case e: Log2 => buildScalarFunction(ScalarFunction.Log2, e.children, e.dataType)
      case e: Log10 => buildScalarFunction(ScalarFunction.Log10, e.children, e.dataType)
      case e: Floor => buildScalarFunction(ScalarFunction.Floor, e.children, e.dataType)
      case e: Ceil => buildScalarFunction(ScalarFunction.Ceil, e.children, e.dataType)
      case Round(_1, Literal(0, _)) =>
        buildScalarFunction(ScalarFunction.Round, Seq(_1), _1.dataType)
      // case Nothing => buildScalarFunction(ScalarFunction.TRUNC, Nil)
      case e: Abs => buildScalarFunction(ScalarFunction.Abs, e.children, e.dataType)
      case e: Signum => buildScalarFunction(ScalarFunction.Signum, e.children, e.dataType)
      case e: OctetLength =>
        buildScalarFunction(ScalarFunction.OctetLength, e.children, e.dataType)
      case e: Concat => buildScalarFunction(ScalarFunction.Concat, e.children, e.dataType)
      case e: Lower => buildScalarFunction(ScalarFunction.Lower, e.children, e.dataType)
      case e: Upper => buildScalarFunction(ScalarFunction.Upper, e.children, e.dataType)
      case e: StringTrim =>
        buildScalarFunction(ScalarFunction.Trim, e.srcStr +: e.trimStr.toSeq, e.dataType)
      case e: StringTrimLeft =>
        buildScalarFunction(ScalarFunction.Ltrim, e.srcStr +: e.trimStr.toSeq, e.dataType)
      case e: StringTrimRight =>
        buildScalarFunction(ScalarFunction.Rtrim, e.srcStr +: e.trimStr.toSeq, e.dataType)
      // case Nothing => buildScalarFunction(ScalarFunction.TOTIMESTAMP, Nil)
      // case Nothing => buildScalarFunction(ScalarFunction.ARRAY, Nil)
      case e: NullIf => buildScalarFunction(ScalarFunction.NullIf, e.children, e.dataType)
      case e: DatePart => buildScalarFunction(ScalarFunction.DatePart, e.children, e.dataType)
      case e: TruncDate => buildScalarFunction(ScalarFunction.DateTrunc, e.children, e.dataType)
      case Md5(_1) =>
        buildScalarFunction(ScalarFunction.MD5, Seq(unpackBinaryTypeCast(_1)), StringType)
      case Sha2(_1, Literal(224, _)) =>
        buildScalarFunction(ScalarFunction.SHA224, Seq(unpackBinaryTypeCast(_1)), StringType)
      case Sha2(_1, Literal(0, _)) =>
        buildScalarFunction(ScalarFunction.SHA256, Seq(unpackBinaryTypeCast(_1)), StringType)
      case Sha2(_1, Literal(256, _)) =>
        buildScalarFunction(ScalarFunction.SHA256, Seq(unpackBinaryTypeCast(_1)), StringType)
      case Sha2(_1, Literal(384, _)) =>
        buildScalarFunction(ScalarFunction.SHA384, Seq(unpackBinaryTypeCast(_1)), StringType)
      case Sha2(_1, Literal(512, _)) =>
        buildScalarFunction(ScalarFunction.SHA512, Seq(unpackBinaryTypeCast(_1)), StringType)
      // case Nothing => buildScalarFunction(ScalarFunction.TOTIMESTAMPMILLIS, Nil)
      case StartsWith(_1, _2) =>
        buildScalarFunction(ScalarFunction.StartsWith, Seq(_1, _2), BooleanType)
      case CaseWhen(branches, elseValue) =>
        val caseExpr = PhysicalCaseNode.newBuilder()
        val whenThens = branches.map {
          case (w, t) =>
            val whenThen = PhysicalWhenThen.newBuilder()
            whenThen.setWhenExpr(convertExpr(w))
            whenThen.setThenExpr(convertExpr(t))
            whenThen.build()
        }
        caseExpr.addAllWhenThenExpr(whenThens.asJava)
        elseValue.foreach(el => caseExpr.setElseExpr(convertExpr(el)))
        PhysicalExprNode.newBuilder().setCase(caseExpr).build()
      case e: Substring =>
        val newChildren = e.children.head +: e.children.tail.map {
          case c =>
            if (c.dataType != LongType) {
              Cast(c, LongType)
            } else c
        }
        buildScalarFunction(ScalarFunction.Substr, newChildren, e.dataType)
      case e: Coalesce => buildScalarFunction(ScalarFunction.Coalesce, e.children, e.dataType)
      case unsupportedExpression =>
        throw new NotImplementedError(s"unsupported exception: ${unsupportedExpression}")
    }
  }

  def convertExprLogical(sparkExpr: Expression): LogicalExprNode = {
    def buildExprNode(
        buildFn: (LogicalExprNode.Builder) => LogicalExprNode.Builder): LogicalExprNode =
      buildFn(LogicalExprNode.newBuilder()).build()

    def buildBinaryExprNode(left: Expression, right: Expression, op: String): LogicalExprNode =
      buildExprNode {
        _.setBinaryExpr(
          BinaryExprNode
            .newBuilder()
            .setL(convertExprLogical(left))
            .setR(convertExprLogical(right))
            .setOp(op))
      }

    def buildScalarFunction(
        fn: ScalarFunction,
        args: Seq[Expression],
        dataType: DataType): LogicalExprNode = buildExprNode {
      _.setScalarFunction(
        ScalarFunctionNode
          .newBuilder()
          .setFun(fn)
          .addAllArgs(args.map(convertExprLogical).asJava))
    }

    def unpackBinaryTypeCast(expr: Expression) = expr match {
      case Cast(inner, BinaryType, _) => inner
      case expr => expr
    }

    sparkExpr match {
      case l @ Literal(value, dataType) =>
        buildExprNode { b =>
          if (!l.nullable) {
            b.setLiteral(convertValue(value, dataType))
          } else {
            b.setLiteral(ScalarValue.newBuilder().setNullValue(convertToScalarType(dataType)))
          }
        }
      case AttributeReference(name, _, _, _) =>
        buildExprNode {
          _.setColumn(Column.newBuilder().setName(name))
        }

      // cast
      case Cast(child, dataType, _) =>
        buildExprNode {
          _.setCast(
            CastNode
              .newBuilder()
              .setExpr(convertExprLogical(child))
              .setArrowType(convertDataType(dataType)))
        }

      // in
      case In(value, list) =>
        buildExprNode {
          _.setInList(
            InListNode
              .newBuilder()
              .setExpr(convertExprLogical(value))
              .addAllList(list.map(convertExprLogical).asJava))
        }

      // inset
      case InSet(value, set) =>
        buildExprNode {
          _.setInList(
            InListNode
              .newBuilder()
              .setExpr(convertExprLogical(value))
              .addAllList(set.map {
                case utf8string: UTF8String => convertExprLogical(Literal(utf8string, StringType))
                case v => convertExprLogical(Literal.apply(v))
              }.asJava))
        }

      // unary ops
      case IsNull(child) =>
        buildExprNode {
          _.setIsNullExpr(
            org.blaze.protobuf.IsNull.newBuilder().setExpr(convertExprLogical(child)).build())
        }
      case IsNotNull(child) =>
        buildExprNode {
          _.setIsNotNullExpr(
            org.blaze.protobuf.IsNotNull.newBuilder().setExpr(convertExprLogical(child)))
        }
      case Not(child) =>
        buildExprNode {
          _.setNotExpr(org.blaze.protobuf.Not.newBuilder().setExpr(convertExprLogical(child)))
        }

      // binary ops
      case EqualTo(lhs, rhs) => buildBinaryExprNode(lhs, rhs, "Eq")
      case GreaterThan(lhs, rhs) => buildBinaryExprNode(lhs, rhs, "Gt")
      case LessThan(lhs, rhs) => buildBinaryExprNode(lhs, rhs, "Lt")
      case GreaterThanOrEqual(lhs, rhs) => buildBinaryExprNode(lhs, rhs, "GtEq")
      case LessThanOrEqual(lhs, rhs) => buildBinaryExprNode(lhs, rhs, "LtEq")
      case Add(lhs, rhs) => buildBinaryExprNode(lhs, rhs, "Plus")
      case Subtract(lhs, rhs) => buildBinaryExprNode(lhs, rhs, "Minus")
      case Multiply(lhs, rhs) => buildBinaryExprNode(lhs, rhs, "Multiply")
      case Divide(lhs, rhs) => buildBinaryExprNode(lhs, rhs, "Divide")
      case Remainder(lhs, rhs) => buildBinaryExprNode(lhs, rhs, "Modulo")
      case Like(lhs, rhs, '\\') => buildBinaryExprNode(lhs, rhs, "Like")
      case And(lhs, rhs) => buildBinaryExprNode(lhs, rhs, "And")
      case Or(lhs, rhs) => buildBinaryExprNode(lhs, rhs, "Or")

      // builtin scalar functions
      case e: Sqrt => buildScalarFunction(ScalarFunction.Sqrt, e.children, e.dataType)
      case e: Sin => buildScalarFunction(ScalarFunction.Sin, e.children, e.dataType)
      case e: Cos => buildScalarFunction(ScalarFunction.Cos, e.children, e.dataType)
      case e: Tan => buildScalarFunction(ScalarFunction.Tan, e.children, e.dataType)
      case e: Asin => buildScalarFunction(ScalarFunction.Asin, e.children, e.dataType)
      case e: Acos => buildScalarFunction(ScalarFunction.Acos, e.children, e.dataType)
      case e: Atan => buildScalarFunction(ScalarFunction.Atan, e.children, e.dataType)
      case e: Exp => buildScalarFunction(ScalarFunction.Exp, e.children, e.dataType)
      case e: Log => buildScalarFunction(ScalarFunction.Log, e.children, e.dataType)
      case e: Log2 => buildScalarFunction(ScalarFunction.Log2, e.children, e.dataType)
      case e: Log10 => buildScalarFunction(ScalarFunction.Log10, e.children, e.dataType)
      case e: Floor => buildScalarFunction(ScalarFunction.Floor, e.children, e.dataType)
      case e: Ceil => buildScalarFunction(ScalarFunction.Ceil, e.children, e.dataType)
      case Round(_1, Literal(0, _)) =>
        buildScalarFunction(ScalarFunction.Round, Seq(_1), _1.dataType)
      // case Nothing => buildScalarFunction(ScalarFunction.TRUNC, Nil)
      case e: Abs => buildScalarFunction(ScalarFunction.Abs, e.children, e.dataType)
      case e: Signum => buildScalarFunction(ScalarFunction.Signum, e.children, e.dataType)
      case e: OctetLength =>
        buildScalarFunction(ScalarFunction.OctetLength, e.children, e.dataType)
      case e: Concat => buildScalarFunction(ScalarFunction.Concat, e.children, e.dataType)
      case e: Lower => buildScalarFunction(ScalarFunction.Lower, e.children, e.dataType)
      case e: Upper => buildScalarFunction(ScalarFunction.Upper, e.children, e.dataType)
      case e: StringTrim =>
        buildScalarFunction(ScalarFunction.Trim, e.srcStr +: e.trimStr.toSeq, e.dataType)
      case e: StringTrimLeft =>
        buildScalarFunction(ScalarFunction.Ltrim, e.srcStr +: e.trimStr.toSeq, e.dataType)
      case e: StringTrimRight =>
        buildScalarFunction(ScalarFunction.Rtrim, e.srcStr +: e.trimStr.toSeq, e.dataType)
      // case Nothing => buildScalarFunction(ScalarFunction.TOTIMESTAMP, Nil)
      // case Nothing => buildScalarFunction(ScalarFunction.ARRAY, Nil)
      case e: NullIf => buildScalarFunction(ScalarFunction.NullIf, e.children, e.dataType)
      case e: DatePart => buildScalarFunction(ScalarFunction.DatePart, e.children, e.dataType)
      case e: TruncDate => buildScalarFunction(ScalarFunction.DateTrunc, e.children, e.dataType)
      case Md5(_1) =>
        buildScalarFunction(ScalarFunction.MD5, Seq(unpackBinaryTypeCast(_1)), StringType)
      case Sha2(_1, Literal(224, _)) =>
        buildScalarFunction(ScalarFunction.SHA224, Seq(unpackBinaryTypeCast(_1)), StringType)
      case Sha2(_1, Literal(0, _)) =>
        buildScalarFunction(ScalarFunction.SHA256, Seq(unpackBinaryTypeCast(_1)), StringType)
      case Sha2(_1, Literal(256, _)) =>
        buildScalarFunction(ScalarFunction.SHA256, Seq(unpackBinaryTypeCast(_1)), StringType)
      case Sha2(_1, Literal(384, _)) =>
        buildScalarFunction(ScalarFunction.SHA384, Seq(unpackBinaryTypeCast(_1)), StringType)
      case Sha2(_1, Literal(512, _)) =>
        buildScalarFunction(ScalarFunction.SHA512, Seq(unpackBinaryTypeCast(_1)), StringType)
      // case Nothing => buildScalarFunction(ScalarFunction.TOTIMESTAMPMILLIS, Nil)
      case StartsWith(_1, _2) =>
        buildScalarFunction(ScalarFunction.StartsWith, Seq(_1, _2), BooleanType)
      case CaseWhen(branches, elseValue) =>
        val caseExpr = CaseNode.newBuilder()
        val whenThens = branches.map {
          case (w, t) =>
            val whenThen = WhenThen.newBuilder()
            whenThen.setWhenExpr(convertExprLogical(w))
            whenThen.setThenExpr(convertExprLogical(t))
            whenThen.build()
        }
        caseExpr.addAllWhenThenExpr(whenThens.asJava)
        elseValue.foreach(el => caseExpr.setElseExpr(convertExprLogical(el)))
        LogicalExprNode.newBuilder().setCase(caseExpr).build()
      case e: Substring => buildScalarFunction(ScalarFunction.Substr, e.children, e.dataType)
      case e: Coalesce => buildScalarFunction(ScalarFunction.Coalesce, e.children, e.dataType)
      case unsupportedExpression =>
        throw new NotImplementedError(s"unsupported exception: ${unsupportedExpression}")
    }
  }

  def convertJoinType(joinType: JoinType): org.blaze.protobuf.JoinType = {
    joinType match {
      case Inner => org.blaze.protobuf.JoinType.INNER
      case LeftOuter => org.blaze.protobuf.JoinType.LEFT
      case RightOuter => org.blaze.protobuf.JoinType.RIGHT
      case FullOuter => org.blaze.protobuf.JoinType.FULL
      case LeftSemi => org.blaze.protobuf.JoinType.SEMI
      case LeftAnti => org.blaze.protobuf.JoinType.ANTI
      case _ => throw new NotImplementedError(s"unsupported join type: ${joinType}")
    }
  }
}
