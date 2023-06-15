/*
 * Copyright 2022 The Blaze Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.blaze

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import scala.collection.JavaConverters._
import scala.collection.mutable
import com.google.protobuf.ByteString
import org.apache.spark.SparkEnv
import org.blaze.{protobuf => pb}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{
  Abs,
  Acos,
  Add,
  Alias,
  And,
  Asin,
  Atan,
  AttributeReference,
  BitwiseAnd,
  BitwiseOr,
  BoundReference,
  CaseWhen,
  Cast,
  Ceil,
  CheckOverflow,
  Coalesce,
  Concat,
  ConcatWs,
  Contains,
  Cos,
  CreateArray,
  CreateNamedStruct,
  Divide,
  EndsWith,
  EqualTo,
  Exp,
  Expression,
  Floor,
  GetArrayItem,
  GetMapValue,
  GetStructField,
  GreaterThan,
  GreaterThanOrEqual,
  If,
  In,
  InSet,
  IsNotNull,
  IsNull,
  Length,
  LessThan,
  LessThanOrEqual,
  Like,
  Literal,
  Log,
  Log10,
  Log2,
  Lower,
  MakeDecimal,
  Md5,
  Multiply,
  Murmur3Hash,
  Not,
  NullIf,
  OctetLength,
  Or,
  Pmod,
  PromotePrecision,
  Remainder,
  Sha2,
  ShiftLeft,
  ShiftRight,
  Signum,
  Sin,
  Sqrt,
  StartsWith,
  StringRepeat,
  StringSpace,
  StringSplit,
  StringTrim,
  StringTrimLeft,
  StringTrimRight,
  Substring,
  Subtract,
  Tan,
  TruncDate,
  Unevaluable,
  UnscaledValue,
  Upper
}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.Average
import org.apache.spark.sql.catalyst.expressions.aggregate.CollectList
import org.apache.spark.sql.catalyst.expressions.aggregate.CollectSet
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.expressions.aggregate.Max
import org.apache.spark.sql.catalyst.expressions.aggregate.Min
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.catalyst.plans.FullOuter
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.LeftAnti
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.catalyst.plans.LeftSemi
import org.apache.spark.sql.catalyst.plans.RightOuter
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.execution.blaze.plan.Util
import org.apache.spark.sql.execution.ScalarSubquery
import org.apache.spark.sql.hive.blaze.HiveUDFUtil
import org.apache.spark.sql.hive.blaze.HiveUDFUtil.getFunctionClassName
import org.apache.spark.sql.hive.blaze.HiveUDFUtil.isHiveSimpleUDF
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.AtomicType
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
import org.apache.spark.sql.types.MapType
import org.apache.spark.sql.types.NullType
import org.apache.spark.sql.types.ShortType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

object NativeConverters extends Logging {

  def convertScalarType(dataType: DataType): pb.ScalarType = {
    val scalarTypeBuilder = dataType match {
      case NullType => pb.ScalarType.newBuilder().setScalar(pb.PrimitiveScalarType.NULL)
      case BooleanType => pb.ScalarType.newBuilder().setScalar(pb.PrimitiveScalarType.BOOL)
      case ByteType => pb.ScalarType.newBuilder().setScalar(pb.PrimitiveScalarType.INT8)
      case ShortType => pb.ScalarType.newBuilder().setScalar(pb.PrimitiveScalarType.INT16)
      case IntegerType => pb.ScalarType.newBuilder().setScalar(pb.PrimitiveScalarType.INT32)
      case LongType => pb.ScalarType.newBuilder().setScalar(pb.PrimitiveScalarType.INT64)
      case FloatType => pb.ScalarType.newBuilder().setScalar(pb.PrimitiveScalarType.FLOAT32)
      case DoubleType => pb.ScalarType.newBuilder().setScalar(pb.PrimitiveScalarType.FLOAT64)
      case StringType => pb.ScalarType.newBuilder().setScalar(pb.PrimitiveScalarType.UTF8)
      case DateType => pb.ScalarType.newBuilder().setScalar(pb.PrimitiveScalarType.DATE32)
      case TimestampType =>
        pb.ScalarType.newBuilder().setScalar(pb.PrimitiveScalarType.TIMESTAMP_MICROSECOND)
      case _: DecimalType =>
        pb.ScalarType.newBuilder().setScalar(pb.PrimitiveScalarType.DECIMAL128)
      case at: ArrayType =>
        pb.ScalarType
          .newBuilder()
          .setList(
            pb.ScalarListType
              .newBuilder()
              .setElementType(convertScalarType(at.elementType)))

      case _ => throw new NotImplementedError(s"Value conversion not implemented ${dataType}")
    }
    scalarTypeBuilder.build()
  }

  def convertDataType(sparkDataType: DataType): pb.ArrowType = {
    val arrowTypeBuilder = pb.ArrowType.newBuilder()
    sparkDataType match {
      case NullType => arrowTypeBuilder.setNONE(pb.EmptyMessage.getDefaultInstance)
      case BooleanType => arrowTypeBuilder.setBOOL(pb.EmptyMessage.getDefaultInstance)
      case ByteType => arrowTypeBuilder.setINT8(pb.EmptyMessage.getDefaultInstance)
      case ShortType => arrowTypeBuilder.setINT16(pb.EmptyMessage.getDefaultInstance)
      case IntegerType => arrowTypeBuilder.setINT32(pb.EmptyMessage.getDefaultInstance)
      case LongType => arrowTypeBuilder.setINT64(pb.EmptyMessage.getDefaultInstance)
      case FloatType => arrowTypeBuilder.setFLOAT32(pb.EmptyMessage.getDefaultInstance)
      case DoubleType => arrowTypeBuilder.setFLOAT64(pb.EmptyMessage.getDefaultInstance)
      case StringType => arrowTypeBuilder.setUTF8(pb.EmptyMessage.getDefaultInstance)
      case BinaryType => arrowTypeBuilder.setBINARY(pb.EmptyMessage.getDefaultInstance)
      case DateType => arrowTypeBuilder.setDATE32(pb.EmptyMessage.getDefaultInstance)

      // timezone is never used in native side
      case TimestampType =>
        arrowTypeBuilder.setTIMESTAMP(
          pb.Timestamp.newBuilder().setTimeUnit(pb.TimeUnit.Microsecond))

      // decimal
      case t: DecimalType =>
        arrowTypeBuilder.setDECIMAL(
          org.blaze.protobuf.Decimal
            .newBuilder()
            .setWhole(Math.max(t.precision, 1))
            .setFractional(t.scale)
            .build())

      // array/list
      case a: ArrayType =>
        typedCheckChildTypeNested(a.elementType)
        arrowTypeBuilder.setLIST(
          org.blaze.protobuf.List
            .newBuilder()
            .setFieldType(
              pb.Field
                .newBuilder()
                .setName("item")
                .setArrowType(convertDataType(a.elementType))
                .setNullable(a.containsNull))
            .build())

      case m: MapType =>
        typedCheckChildTypeNested(m.keyType)
        typedCheckChildTypeNested(m.valueType)
        arrowTypeBuilder.setMAP(
          org.blaze.protobuf.Map
            .newBuilder()
            .setKeyType(
              pb.Field
                .newBuilder()
                .setName("key")
                .setArrowType(convertDataType(m.keyType))
                .setNullable(false))
            .setValueType(
              pb.Field
                .newBuilder()
                .setName("value")
                .setArrowType(convertDataType(m.valueType))
                .setNullable(m.valueContainsNull))
            .build())
      case s: StructType =>
        s.fields.foreach(field => typedCheckChildTypeNested(field.dataType))
        arrowTypeBuilder.setSTRUCT(
          org.blaze.protobuf.Struct
            .newBuilder()
            .addAllSubFieldTypes(
              s.fields
                .map(
                  e =>
                    pb.Field
                      .newBuilder()
                      .setArrowType(convertDataType(e.dataType))
                      .setName(e.name)
                      .setNullable(e.nullable)
                      .build())
                .toList
                .asJava)
            .build())

      case _ =>
        throw new NotImplementedError(s"Data type conversion not implemented ${sparkDataType}")
    }
    arrowTypeBuilder.build()
  }

  def convertValue(sparkValue: Any, dataType: DataType): pb.ScalarValue = {
    val scalarValueBuilder = pb.ScalarValue.newBuilder()
    dataType match {
      case _ if sparkValue == null => scalarValueBuilder.setNullValue(convertScalarType(dataType))
      case BooleanType => scalarValueBuilder.setBoolValue(sparkValue.asInstanceOf[Boolean])
      case ByteType => scalarValueBuilder.setInt8Value(sparkValue.asInstanceOf[Byte])
      case ShortType => scalarValueBuilder.setInt16Value(sparkValue.asInstanceOf[Short])
      case IntegerType => scalarValueBuilder.setInt32Value(sparkValue.asInstanceOf[Int])
      case LongType => scalarValueBuilder.setInt64Value(sparkValue.asInstanceOf[Long])
      case FloatType => scalarValueBuilder.setFloat32Value(sparkValue.asInstanceOf[Float])
      case DoubleType => scalarValueBuilder.setFloat64Value(sparkValue.asInstanceOf[Double])
      case StringType => scalarValueBuilder.setUtf8Value(sparkValue.toString)
      case DateType => scalarValueBuilder.setDate32Value(sparkValue.asInstanceOf[Int])
      case TimestampType =>
        scalarValueBuilder.setTimestampMicrosecondValue(sparkValue.asInstanceOf[Long])
      case t: DecimalType =>
        val decimalValue = sparkValue.asInstanceOf[Decimal]
        val decimalType = convertDataType(t).getDECIMAL
        scalarValueBuilder.setDecimalValue(
          pb.ScalarDecimalValue
            .newBuilder()
            .setDecimal(decimalType)
            .setLongValue(decimalValue.toUnscaledLong))

      case at: ArrayType =>
        val values =
          pb.ScalarListValue.newBuilder().setDatatype(convertScalarType(at.elementType))
        sparkValue
          .asInstanceOf[ArrayData]
          .foreach(
            at.elementType,
            (_, value) => {
              values.addValues(convertValue(value, at.elementType))
            })
        scalarValueBuilder.setListValue(values)
    }
    scalarValueBuilder.build()
  }

  def convertField(sparkField: StructField): pb.Field = {
    pb.Field
      .newBuilder()
      .setName(sparkField.name)
      .setNullable(sparkField.nullable)
      .setArrowType(convertDataType(sparkField.dataType))
      .build()
  }

  def convertSchema(sparkSchema: StructType): pb.Schema = {
    val schemaBuilder = pb.Schema.newBuilder()
    sparkSchema.foreach(sparkField => schemaBuilder.addColumns(convertField(sparkField)))
    schemaBuilder.build()
  }

  case class NativeExprWrapper(
      wrapped: pb.PhysicalExprNode,
      override val dataType: DataType = NullType,
      override val nullable: Boolean = true,
      override val children: Seq[Expression] = Nil)
      extends Unevaluable

  def convertExpr(sparkExpr: Expression): pb.PhysicalExprNode = {
    def fallbackToError: Expression => pb.PhysicalExprNode = { e =>
      throw new NotImplementedError(s"unsupported expression: (${e.getClass}) $e")
    }

    try {
      // get number of inconvertible children
      var numInconvertibleChildren = 0
      sparkExpr.children.foreach { child =>
        try {
          convertExprWithFallback(child, isPruningExpr = false, fallbackToError)
        } catch {
          case _: NotImplementedError =>
            numInconvertibleChildren += 1
        }
      }

      // number of inconvertible children:
      //  0 - try convert the whole expression
      //  1 - fallback the only inconvertible children
      //  N - fallback the whole expression
      numInconvertibleChildren match {
        case 0 => convertExprWithFallback(sparkExpr, isPruningExpr = false, fallbackToError)
        case 1 =>
          val childrenConverted = sparkExpr.mapChildren { child =>
            try {
              val converted =
                convertExprWithFallback(child, isPruningExpr = false, fallbackToError)
              NativeExprWrapper(converted, child.dataType, child.nullable)
            } catch {
              case _: NotImplementedError =>
                val fallbacked = convertExpr(child)
                NativeExprWrapper(fallbacked, child.dataType, child.nullable)
            }
          }
          convertExprWithFallback(childrenConverted, isPruningExpr = false, fallbackToError)
        case _ =>
          fallbackToError(sparkExpr)
      }

    } catch {
      case e: NotImplementedError =>
        logWarning(s"native expression fallbacks to spark: $e")

        // bind all convertible children
        val convertedChildren = mutable.LinkedHashMap[pb.PhysicalExprNode, BoundReference]()
        val bound = sparkExpr.mapChildren(_.transformDown {
          case p: Literal => p
          case p =>
            try {
              val convertedChild =
                convertExprWithFallback(p, isPruningExpr = false, fallbackToError)
              val nextBindIndex = convertedChildren.size
              convertedChildren.getOrElseUpdate(
                convertedChild,
                BoundReference(nextBindIndex, p.dataType, p.nullable))
            } catch {
              case _: Exception | _: NotImplementedError => p
            }
        })

        val paramsSchema = StructType(
          convertedChildren.values
            .map(ref => StructField("", ref.dataType, ref.nullable))
            .toSeq)

        val serialized =
          serializeExpression(bound.asInstanceOf[Expression with Serializable], paramsSchema)

        // build SparkUDFWrapperExpr
        pb.PhysicalExprNode
          .newBuilder()
          .setSparkUdfWrapperExpr(
            pb.PhysicalSparkUDFWrapperExprNode
              .newBuilder()
              .setSerialized(ByteString.copyFrom(serialized))
              .setReturnType(convertDataType(bound.dataType))
              .setReturnNullable(bound.nullable)
              .addAllParams(convertedChildren.keys.asJava))
          .build()
    }
  }

  def convertScanPruningExpr(sparkExpr: Expression): pb.PhysicalExprNode = {
    convertExprWithFallback(
      sparkExpr,
      isPruningExpr = true,
      { _ =>
        buildExprNode(
          _.setColumn(
            pb.PhysicalColumn
              .newBuilder()
              .setName("!__unsupported_pruning_expr__")
              .setIndex(Int.MaxValue)))
      })
  }

  private def buildExprNode(buildFn: pb.PhysicalExprNode.Builder => pb.PhysicalExprNode.Builder)
      : pb.PhysicalExprNode = {
    buildFn(pb.PhysicalExprNode.newBuilder()).build()
  }

  private def convertExprWithFallback(
      sparkExpr: Expression,
      isPruningExpr: Boolean = false,
      fallback: Expression => pb.PhysicalExprNode): pb.PhysicalExprNode = {
    assert(sparkExpr.deterministic, s"nondeterministic expression not supported: $sparkExpr")

    def buildBinaryExprNode(
        left: Expression,
        right: Expression,
        op: String): pb.PhysicalExprNode =
      buildExprNode {
        _.setBinaryExpr(
          pb.PhysicalBinaryExprNode
            .newBuilder()
            .setL(convertExprWithFallback(left, isPruningExpr, fallback))
            .setR(convertExprWithFallback(right, isPruningExpr, fallback))
            .setOp(op))
      }

    def buildScalarFunction(
        fn: pb.ScalarFunction,
        args: Seq[Expression],
        dataType: DataType): pb.PhysicalExprNode =
      buildExprNode {
        _.setScalarFunction(
          pb.PhysicalScalarFunctionNode
            .newBuilder()
            .setName(fn.name())
            .setFun(fn)
            .addAllArgs(
              args.map(expr => convertExprWithFallback(expr, isPruningExpr, fallback)).asJava)
            .setReturnType(convertDataType(dataType)))
      }

    def buildExtScalarFunction(
        name: String,
        args: Seq[Expression],
        dataType: DataType): pb.PhysicalExprNode =
      buildExprNode {
        _.setScalarFunction(
          pb.PhysicalScalarFunctionNode
            .newBuilder()
            .setName(name)
            .setFun(pb.ScalarFunction.SparkExtFunctions)
            .addAllArgs(
              args.map(expr => convertExprWithFallback(expr, isPruningExpr, fallback)).asJava)
            .setReturnType(convertDataType(dataType)))
      }

    def castIfNecessary(expr: Expression, dataType: DataType): Expression = {
      if (expr.dataType == dataType) {
        return expr
      }
      Cast(expr, dataType)
    }

    def unpackBinaryTypeCast(expr: Expression) =
      expr match {
        case Cast(inner, BinaryType, _) => inner
        case expr => expr
      }

    sparkExpr match {
      case NativeExprWrapper(wrapped, _, _, _) =>
        wrapped
      case Literal(value, dataType) =>
        buildExprNode { b =>
          if (value == null) {
            dataType match {
              case at: ArrayType =>
                b.setTryCast(
                  pb.PhysicalTryCastNode
                    .newBuilder()
                    .setArrowType(convertDataType(at))
                    .setExpr(buildExprNode {
                      _.setLiteral(
                        pb.ScalarValue.newBuilder().setNullValue(convertScalarType(NullType)))
                    }))
              case _ =>
                b.setLiteral(
                  pb.ScalarValue.newBuilder().setNullValue(convertScalarType(dataType)))
            }
          } else {
            b.setLiteral(convertValue(value, dataType))
          }
        }

      case bound: BoundReference =>
        buildExprNode {
          _.setBoundReference(
            pb.BoundReference
              .newBuilder()
              .setIndex(bound.ordinal)
              .setDataType(convertDataType(bound.dataType))
              .setNullable(bound.nullable))
        }

      // use column name is pruning-expr mode and column expr id in normal mode
      case ar: AttributeReference if isPruningExpr =>
        buildExprNode(_.setColumn(pb.PhysicalColumn.newBuilder().setName(ar.name)))
      case ar: AttributeReference =>
        buildExprNode {
          _.setColumn(
            pb.PhysicalColumn
              .newBuilder()
              .setName(Util.getFieldNameByExprId(ar))
              .build())
        }

      case alias: Alias =>
        convertExprWithFallback(alias.child, isPruningExpr, fallback)

      // ScalarSubquery
      case subquery: ScalarSubquery =>
        subquery.updateResult()
        val value = Literal.create(subquery.eval(null), subquery.dataType)
        convertExprWithFallback(value, isPruningExpr, fallback)

      // cast
      // not performing native cast for timestamps (will use UDFWrapper instead)
      case Cast(child, dataType, _) if !Seq(dataType, child.dataType).contains(TimestampType) =>
        buildExprNode {
          _.setTryCast(
            pb.PhysicalTryCastNode
              .newBuilder()
              .setExpr(convertExprWithFallback(child, isPruningExpr, fallback))
              .setArrowType(convertDataType(dataType))
              .build())
        }

      // in
      case In(value, list) if list.forall(_.isInstanceOf[Literal]) =>
        buildExprNode {
          _.setInList(
            pb.PhysicalInListNode
              .newBuilder()
              .setExpr(convertExprWithFallback(value, isPruningExpr, fallback))
              .addAllList(
                list.map(expr => convertExprWithFallback(expr, isPruningExpr, fallback)).asJava))
        }

      // in
      case InSet(value, set) if set.forall(_.isInstanceOf[Literal]) =>
        buildExprNode {
          _.setInList(
            pb.PhysicalInListNode
              .newBuilder()
              .setExpr(convertExprWithFallback(value, isPruningExpr, fallback))
              .addAllList(set.map {
                case utf8string: UTF8String =>
                  convertExprWithFallback(
                    Literal(utf8string, StringType),
                    isPruningExpr,
                    fallback)
                case v => convertExprWithFallback(Literal.apply(v), isPruningExpr, fallback)
              }.asJava))
        }

      // unary ops
      case IsNull(child) =>
        buildExprNode {
          _.setIsNullExpr(
            pb.PhysicalIsNull
              .newBuilder()
              .setExpr(convertExprWithFallback(child, isPruningExpr, fallback))
              .build())
        }
      case IsNotNull(child) =>
        buildExprNode {
          _.setIsNotNullExpr(
            pb.PhysicalIsNotNull
              .newBuilder()
              .setExpr(convertExprWithFallback(child, isPruningExpr, fallback))
              .build())
        }
      case Not(EqualTo(lhs, rhs)) => buildBinaryExprNode(lhs, rhs, "NotEq")
      case Not(child) =>
        buildExprNode {
          _.setNotExpr(
            pb.PhysicalNot
              .newBuilder()
              .setExpr(convertExprWithFallback(child, isPruningExpr, fallback))
              .build())
        }

      // binary ops
      case EqualTo(lhs, rhs) => buildBinaryExprNode(lhs, rhs, "Eq")
      case GreaterThan(lhs, rhs) => buildBinaryExprNode(lhs, rhs, "Gt")
      case LessThan(lhs, rhs) => buildBinaryExprNode(lhs, rhs, "Lt")
      case GreaterThanOrEqual(lhs, rhs) => buildBinaryExprNode(lhs, rhs, "GtEq")
      case LessThanOrEqual(lhs, rhs) => buildBinaryExprNode(lhs, rhs, "LtEq")

      case e @ Add(lhs, rhs) =>
        if (lhs.dataType.isInstanceOf[DecimalType] || rhs.dataType.isInstanceOf[DecimalType]) {
          val resultType = arithDecimalReturnType(e)
          val left = Cast(lhs, DoubleType)
          val right = Cast(rhs, DoubleType)
          buildExprNode {
            _.setTryCast(
              pb.PhysicalTryCastNode
                .newBuilder()
                .setExpr(convertExprWithFallback(Add.apply(left, right), isPruningExpr, fallback))
                .setArrowType(convertDataType(resultType))
                .build())
          }
        } else {
          buildBinaryExprNode(lhs, rhs, "Plus")
        }

      case e @ Subtract(lhs, rhs) =>
        if (lhs.dataType.isInstanceOf[DecimalType] || rhs.dataType.isInstanceOf[DecimalType]) {
          val resultType = arithDecimalReturnType(e)
          val left = Cast(lhs, DoubleType)
          val right = Cast(rhs, DoubleType)
          buildExprNode {
            _.setTryCast(
              pb.PhysicalTryCastNode
                .newBuilder()
                .setExpr(
                  convertExprWithFallback(Subtract.apply(left, right), isPruningExpr, fallback))
                .setArrowType(convertDataType(resultType))
                .build())
          }
        } else {
          buildBinaryExprNode(lhs, rhs, "Minus")
        }

      case e @ Multiply(lhs, rhs) =>
        if (lhs.dataType.isInstanceOf[DecimalType] || rhs.dataType.isInstanceOf[DecimalType]) {
          val resultType = arithDecimalReturnType(e)
          val left = Cast(lhs, DoubleType)
          val right = Cast(rhs, DoubleType)
          buildExprNode {
            _.setTryCast(
              pb.PhysicalTryCastNode
                .newBuilder()
                .setExpr(
                  convertExprWithFallback(Multiply.apply(left, right), isPruningExpr, fallback))
                .setArrowType(convertDataType(resultType))
                .build())
          }
        } else {
          buildBinaryExprNode(lhs, rhs, "Multiply")
        }

      case e @ Divide(lhs, rhs) =>
        if (lhs.dataType.isInstanceOf[DecimalType] || rhs.dataType.isInstanceOf[DecimalType]) {
          val resultType = arithDecimalReturnType(e)
          val left = Cast(lhs, DoubleType)
          val right = Cast(rhs, DoubleType)
          buildExprNode {
            _.setTryCast(
              pb.PhysicalTryCastNode
                .newBuilder()
                .setExpr(
                  convertExprWithFallback(Divide.apply(left, right), isPruningExpr, fallback))
                .setArrowType(convertDataType(resultType))
                .build())
          }
        } else {
          buildBinaryExprNode(lhs, rhs, "Divide")
        }

      case e @ Remainder(lhs, rhs) =>
        val resultType = arithDecimalReturnType(e)
        rhs match {
          case rhs: Literal if rhs == Literal.default(rhs.dataType) =>
            buildExprNode(_.setLiteral(convertValue(null, e.dataType)))
          case rhs: Literal if rhs != Literal.default(rhs.dataType) =>
            buildBinaryExprNode(lhs, rhs, "Modulo")
          case rhs =>
            val lhsCasted = castIfNecessary(lhs, resultType)
            val rhsCasted = castIfNecessary(rhs, resultType)
            buildExprNode {
              _.setBinaryExpr(
                pb.PhysicalBinaryExprNode
                  .newBuilder()
                  .setL(convertExprWithFallback(lhsCasted, isPruningExpr, fallback))
                  .setR(buildExtScalarFunction("NullIfZero", rhsCasted :: Nil, rhs.dataType))
                  .setOp("Modulo"))
            }
        }
      case e: Like =>
        assert(Shims.get.exprShims.getEscapeChar(e) == '\\')
        buildExprNode {
          _.setLikeExpr(
            pb.PhysicalLikeExprNode
              .newBuilder()
              .setNegated(false)
              .setCaseInsensitive(false)
              .setExpr(convertExprWithFallback(e.left, isPruningExpr, fallback))
              .setPattern(convertExprWithFallback(e.right, isPruningExpr, fallback)))
        }

      // if rhs is complex in and/or operators, use short-circuiting implementation
      case e @ And(lhs, rhs) if rhs.find(HiveUDFUtil.isHiveUDF).isDefined =>
        buildExprNode {
          _.setScAndExpr(
            pb.PhysicalSCAndExprNode
              .newBuilder()
              .setLeft(convertExprWithFallback(lhs, isPruningExpr, fallback))
              .setRight(convertExprWithFallback(rhs, isPruningExpr, fallback)))
        }
      case e @ Or(lhs, rhs) if rhs.find(HiveUDFUtil.isHiveUDF).isDefined =>
        buildExprNode {
          _.setScOrExpr(
            pb.PhysicalSCOrExprNode
              .newBuilder()
              .setLeft(convertExprWithFallback(lhs, isPruningExpr, fallback))
              .setRight(convertExprWithFallback(rhs, isPruningExpr, fallback)))
        }
      case And(lhs, rhs) => buildBinaryExprNode(lhs, rhs, "And")
      case Or(lhs, rhs) => buildBinaryExprNode(lhs, rhs, "Or")

      // bitwise
      case BitwiseAnd(lhs, rhs) =>
        buildBinaryExprNode(lhs, castIfNecessary(rhs, lhs.dataType), "BitwiseAnd")
      case BitwiseOr(lhs, rhs) =>
        buildBinaryExprNode(lhs, castIfNecessary(rhs, lhs.dataType), "BitwiseOr")
      case ShiftLeft(lhs, rhs) =>
        buildBinaryExprNode(lhs, castIfNecessary(rhs, lhs.dataType), "BitwiseShiftLeft")
      case ShiftRight(lhs, rhs) =>
        buildBinaryExprNode(lhs, castIfNecessary(rhs, lhs.dataType), "BitwiseShiftRight")

      // builtin scalar functions
      case e: Sqrt => buildScalarFunction(pb.ScalarFunction.Sqrt, e.children, e.dataType)
      case e: Sin => buildScalarFunction(pb.ScalarFunction.Sin, e.children, e.dataType)
      case e: Cos => buildScalarFunction(pb.ScalarFunction.Cos, e.children, e.dataType)
      case e: Tan => buildScalarFunction(pb.ScalarFunction.Tan, e.children, e.dataType)
      case e: Asin => buildScalarFunction(pb.ScalarFunction.Asin, e.children, e.dataType)
      case e: Acos => buildScalarFunction(pb.ScalarFunction.Acos, e.children, e.dataType)
      case e: Atan => buildScalarFunction(pb.ScalarFunction.Atan, e.children, e.dataType)
      case e: Exp => buildScalarFunction(pb.ScalarFunction.Exp, e.children, e.dataType)
      case e: Log => buildScalarFunction(pb.ScalarFunction.Ln, e.children, e.dataType)
      case e: Log2 => buildScalarFunction(pb.ScalarFunction.Log2, e.children, e.dataType)
      case e: Log10 => buildScalarFunction(pb.ScalarFunction.Log10, e.children, e.dataType)
      case e: Floor if !e.dataType.isInstanceOf[DecimalType] =>
        buildExprNode {
          _.setTryCast(
            pb.PhysicalTryCastNode
              .newBuilder()
              .setExpr(buildScalarFunction(pb.ScalarFunction.Floor, e.children, e.dataType))
              .setArrowType(convertDataType(e.dataType))
              .build())
        }
      case e: Ceil if !e.dataType.isInstanceOf[DecimalType] =>
        buildExprNode {
          _.setTryCast(
            pb.PhysicalTryCastNode
              .newBuilder()
              .setExpr(buildScalarFunction(pb.ScalarFunction.Ceil, e.children, e.dataType))
              .setArrowType(convertDataType(e.dataType))
              .build())
        }

      // TODO: datafusion's round() has different behavior from spark
      // case e @ Round(_1, Literal(n: Int, _)) if _1.dataType.isInstanceOf[FractionalType] =>
      //   buildScalarFunction(pb.ScalarFunction.Round, Seq(_1, Literal(n.toLong)), e.dataType)

      case e: Signum => buildScalarFunction(pb.ScalarFunction.Signum, e.children, e.dataType)
      case e: Abs if e.dataType.isInstanceOf[FloatType] || e.dataType.isInstanceOf[DoubleType] =>
        buildScalarFunction(pb.ScalarFunction.Abs, e.children, e.dataType)
      case e: OctetLength =>
        buildScalarFunction(pb.ScalarFunction.OctetLength, e.children, e.dataType)
      case Length(arg) if arg.dataType == StringType =>
        buildScalarFunction(pb.ScalarFunction.CharacterLength, arg :: Nil, IntegerType)

      // TODO: datafusion's upper/lower() has different behavior from spark
//      case e: Lower =>
//        logInfo(s"lower children is: ${e.children} and type is: ${e.dataType}")
//        buildScalarFunction(pb.ScalarFunction.Lower, e.children, e.dataType)
//      case e: Upper => buildScalarFunction(pb.ScalarFunction.Upper, e.children, e.dataType)

      case e: StringTrim =>
        buildScalarFunction(pb.ScalarFunction.Trim, e.srcStr +: e.trimStr.toSeq, e.dataType)
      case e: StringTrimLeft =>
        buildScalarFunction(pb.ScalarFunction.Ltrim, e.srcStr +: e.trimStr.toSeq, e.dataType)
      case e: StringTrimRight =>
        buildScalarFunction(pb.ScalarFunction.Rtrim, e.srcStr +: e.trimStr.toSeq, e.dataType)
      case e @ NullIf(left, right, _) =>
        buildScalarFunction(pb.ScalarFunction.NullIf, left :: right :: Nil, e.dataType)
      case e: TruncDate =>
        buildScalarFunction(pb.ScalarFunction.DateTrunc, e.children, e.dataType)
      case Md5(_1) =>
        buildScalarFunction(pb.ScalarFunction.MD5, Seq(unpackBinaryTypeCast(_1)), StringType)
      case Sha2(_1, Literal(224, _)) =>
        buildScalarFunction(pb.ScalarFunction.SHA224, Seq(unpackBinaryTypeCast(_1)), StringType)
      case Sha2(_1, Literal(0, _)) =>
        buildScalarFunction(pb.ScalarFunction.SHA256, Seq(unpackBinaryTypeCast(_1)), StringType)
      case Sha2(_1, Literal(256, _)) =>
        buildScalarFunction(pb.ScalarFunction.SHA256, Seq(unpackBinaryTypeCast(_1)), StringType)
      case Sha2(_1, Literal(384, _)) =>
        buildScalarFunction(pb.ScalarFunction.SHA384, Seq(unpackBinaryTypeCast(_1)), StringType)
      case Sha2(_1, Literal(512, _)) =>
        buildScalarFunction(pb.ScalarFunction.SHA512, Seq(unpackBinaryTypeCast(_1)), StringType)
      case Murmur3Hash(children, 42) =>
        buildExtScalarFunction("Murmur3Hash", children, IntegerType)

      // startswith is converted to scalar function in pruning-expr mode
      case StartsWith(expr, Literal(prefix, StringType)) if isPruningExpr =>
        buildExprNode(
          _.setScalarFunction(
            pb.PhysicalScalarFunctionNode
              .newBuilder()
              .setName("starts_with")
              .setFun(pb.ScalarFunction.StartsWith)
              .addArgs(convertExprWithFallback(expr, isPruningExpr, fallback))
              .addArgs(
                convertExprWithFallback(Literal(prefix, StringType), isPruningExpr, fallback))
              .setReturnType(convertDataType(BooleanType))))
      case StartsWith(expr, Literal(prefix, StringType)) =>
        buildExprNode(
          _.setStringStartsWithExpr(
            pb.StringStartsWithExprNode
              .newBuilder()
              .setExpr(convertExprWithFallback(expr, isPruningExpr, fallback))
              .setPrefix(prefix.toString)))

      case EndsWith(expr, Literal(suffix, StringType)) =>
        buildExprNode(
          _.setStringEndsWithExpr(
            pb.StringEndsWithExprNode
              .newBuilder()
              .setExpr(convertExprWithFallback(expr, isPruningExpr, fallback))
              .setSuffix(suffix.toString)))

      case Contains(expr, Literal(infix, StringType)) =>
        buildExprNode(
          _.setStringContainsExpr(
            pb.StringContainsExprNode
              .newBuilder()
              .setExpr(convertExprWithFallback(expr, isPruningExpr, fallback))
              .setInfix(infix.toString)))

      case Substring(str, Literal(pos, IntegerType), Literal(len, IntegerType))
          if pos.asInstanceOf[Int] >= 0 && len.asInstanceOf[Int] >= 0 =>
        buildScalarFunction(
          pb.ScalarFunction.Substr,
          str :: Literal(pos.asInstanceOf[Long]) :: Literal(len.asInstanceOf[Long]) :: Nil,
          StringType)

      case StringSpace(n) =>
        buildExtScalarFunction("StringSpace", n :: Nil, StringType)

      case StringRepeat(str, n @ Literal(_, IntegerType)) =>
        buildExtScalarFunction("StringRepeat", str :: n :: Nil, StringType)

      case StringSplit(str, pat @ Literal(_, StringType))
          // native StringSplit implementation does not support regex, so only most frequently
          // used cases without regex are supported
          if Seq(",", ", ", ":", ";", "#", "@", "_", "-", "\\|", "\\.").contains(pat.value) =>
        val nativePat = pat.value match {
          case "\\|" => "|"
          case "\\." => "."
          case other => other
        }
        buildExtScalarFunction(
          "StringSplit",
          str :: Literal(nativePat) :: Nil,
          ArrayType(StringType))

      case e: Concat if e.children.forall(_.dataType == StringType) =>
        buildExtScalarFunction("StringConcat", e.children, e.dataType)

      case e: ConcatWs
          if e.children.forall(
            c => c.dataType == StringType || c.dataType == ArrayType(StringType)) =>
        buildExtScalarFunction("StringConcatWs", e.children, e.dataType)

      case e: Coalesce => buildScalarFunction(pb.ScalarFunction.Coalesce, e.children, e.dataType)

      case e @ If(predicate, trueValue, falseValue) =>
        buildExprNode {
          _.setIifExpr(
            pb.IIfExprNode
              .newBuilder()
              .setCondition(convertExprWithFallback(predicate, isPruningExpr, fallback))
              .setTruthy(convertExprWithFallback(trueValue, isPruningExpr, fallback))
              .setFalsy(convertExprWithFallback(falseValue, isPruningExpr, fallback))
              .setDataType(convertDataType(e.dataType)))
        }
      case CaseWhen(branches, elseValue) =>
        val caseExpr = pb.PhysicalCaseNode.newBuilder()
        val whenThens = branches.map {
          case (w, t) =>
            val whenThen = pb.PhysicalWhenThen.newBuilder()
            whenThen.setWhenExpr(convertExprWithFallback(w, isPruningExpr, fallback))
            whenThen.setThenExpr(convertExprWithFallback(t, isPruningExpr, fallback))
            whenThen.build()
        }
        caseExpr.addAllWhenThenExpr(whenThens.asJava)
        elseValue.foreach(el =>
          caseExpr.setElseExpr(convertExprWithFallback(el, isPruningExpr, fallback)))
        pb.PhysicalExprNode.newBuilder().setCase(caseExpr).build()

      // expressions for DecimalPrecision rule
      case UnscaledValue(_1) =>
        val args = _1 :: Nil
        buildExtScalarFunction("UnscaledValue", args, LongType)

      case e: MakeDecimal =>
        // case MakeDecimal(_1, precision, scale) =>
        //  assert(!SQLConf.get.ansiEnabled)
        val precision = e.precision
        val scale = e.scale
        val args =
          e.child :: Literal
            .apply(precision, IntegerType) :: Literal.apply(scale, IntegerType) :: Nil
        buildExtScalarFunction("MakeDecimal", args, DecimalType(precision, scale))

      case PromotePrecision(_1) =>
        _1 match {
          case Cast(_, dt, _) if dt == _1.dataType =>
            convertExprWithFallback(_1, isPruningExpr, fallback)
          case _ =>
            convertExprWithFallback(Cast(_1, _1.dataType), isPruningExpr, fallback)
        }
      case e: CheckOverflow =>
        // case CheckOverflow(_1, DecimalType(precision, scale)) =>
        val precision = e.dataType.precision
        val scale = e.dataType.scale
        val args =
          e.child :: Literal
            .apply(precision, IntegerType) :: Literal.apply(scale, IntegerType) :: Nil
        buildExtScalarFunction("CheckOverflow", args, DecimalType(precision, scale))

      case e: CreateArray => buildExtScalarFunction("MakeArray", e.children, e.dataType)

      case e: CreateNamedStruct =>
        buildExprNode {
          _.setNamedStruct(
            pb.PhysicalNamedStructExprNode
              .newBuilder()
              .addAllValues(e.valExprs
                .map(value => convertExprWithFallback(value, isPruningExpr, fallback))
                .asJava)
              .setReturnType(convertDataType(e.dataType)))
        }

      case GetArrayItem(child, Literal(ordinalValue: Number, _)) =>
        buildExprNode {
          _.setGetIndexedFieldExpr(
            pb.PhysicalGetIndexedFieldExprNode
              .newBuilder()
              .setExpr(convertExprWithFallback(child, isPruningExpr, fallback))
              .setKey(convertValue(
                ordinalValue.longValue() + 1, // NOTE: data-fusion index starts from 1
                LongType)))
        }

      case GetMapValue(child, Literal(value, dataType)) =>
        buildExprNode {
          _.setGetMapValueExpr(
            pb.PhysicalGetMapValueExprNode
              .newBuilder()
              .setExpr(convertExprWithFallback(child, isPruningExpr, fallback))
              .setKey(convertValue(value, dataType)))
        }

      case GetStructField(child, ordinal, _) =>
        buildExprNode {
          _.setGetIndexedFieldExpr(
            pb.PhysicalGetIndexedFieldExprNode
              .newBuilder()
              .setExpr(convertExprWithFallback(child, isPruningExpr, fallback))
              .setKey(convertValue(ordinal, IntegerType)))
        }

      // hive UDFJson
      case e
          if (isHiveSimpleUDF(e)
            && getFunctionClassName(e).contains("org.apache.hadoop.hive.ql.udf.UDFJson")
            && SparkEnv.get.conf.getBoolean(
              "spark.blaze.udf.UDFJson.enabled",
              defaultValue = true)
            && e.children.length == 2
            && e.children(0).dataType == StringType
            && e.children(1).dataType == StringType
            && e.children(1).isInstanceOf[Literal]) =>
        buildExtScalarFunction("GetJsonObject", e.children, StringType)

      case e =>
        fallback(e)
    }
  }

  def convertAggregateExpr(e: AggregateExpression): pb.PhysicalExprNode = {
    assert(Shims.get.exprShims.getAggregateExpressionFilter(e).isEmpty)
    val aggBuilder = pb.PhysicalAggExprNode.newBuilder()

    e.aggregateFunction match {
      case e @ Max(child) if e.dataType.isInstanceOf[AtomicType] =>
        aggBuilder.setAggFunction(pb.AggFunction.MAX)
        aggBuilder.addChildren(convertExpr(child))
      case e @ Min(child) if e.dataType.isInstanceOf[AtomicType] =>
        aggBuilder.setAggFunction(pb.AggFunction.MIN)
        aggBuilder.addChildren(convertExpr(child))
      case e @ Sum(child) if e.dataType.isInstanceOf[AtomicType] =>
        aggBuilder.setAggFunction(pb.AggFunction.SUM)
        aggBuilder.addChildren(convertExpr(child))
      case e @ Average(child) if e.dataType.isInstanceOf[AtomicType] =>
        aggBuilder.setAggFunction(pb.AggFunction.AVG)
        aggBuilder.addChildren(convertExpr(child))
      case Count(children) if !children.exists(_.nullable) =>
        aggBuilder.setAggFunction(pb.AggFunction.COUNT)
        aggBuilder.addChildren(convertExpr(Literal.apply(1)))
      case Count(children) =>
        aggBuilder.setAggFunction(pb.AggFunction.COUNT)
        if (children.length == 1) {
          aggBuilder.addChildren(convertExpr(children.head))
        } else {
          aggBuilder.addChildren(
            convertExpr(
              If(
                children.filter(_.nullable).map(IsNull).reduce(Or),
                Literal(null, IntegerType),
                Literal(1))))
        }

      case CollectList(child, _, _) if child.dataType.isInstanceOf[AtomicType] =>
        aggBuilder.setAggFunction(pb.AggFunction.COLLECT_LIST)
        aggBuilder.addChildren(convertExpr(child))
      case CollectSet(child, _, _) if child.dataType.isInstanceOf[AtomicType] =>
        aggBuilder.setAggFunction(pb.AggFunction.COLLECT_SET)
        aggBuilder.addChildren(convertExpr(child))
    }
    pb.PhysicalExprNode
      .newBuilder()
      .setAggExpr(aggBuilder)
      .build()
  }

  def convertJoinType(joinType: JoinType): pb.JoinType = {
    joinType match {
      case Inner => pb.JoinType.INNER
      case LeftOuter => pb.JoinType.LEFT
      case RightOuter => pb.JoinType.RIGHT
      case FullOuter => pb.JoinType.FULL
      case LeftSemi => pb.JoinType.SEMI
      case LeftAnti => pb.JoinType.ANTI
      case _ => throw new NotImplementedError(s"unsupported join type: ${joinType}")
    }
  }

  def serializeExpression(
      expr: Expression with Serializable,
      paramsSchema: StructType): Array[Byte] = {
    Utils.tryWithResource(new ByteArrayOutputStream()) { bos =>
      Utils.tryWithResource(new ObjectOutputStream(bos)) { oos =>
        oos.writeObject(expr)
        oos.writeObject(paramsSchema)
        null
      }
      bos.toByteArray
    }
  }

  def deserializeExpression(
      serialized: Array[Byte]): (Expression with Serializable, StructType) = {
    Utils.tryWithResource(new ByteArrayInputStream(serialized)) { bis =>
      Utils.tryWithResource(new ObjectInputStream(bis)) { ois =>
        val expr = ois.readObject().asInstanceOf[Expression with Serializable]
        val paramsSchema = ois.readObject().asInstanceOf[StructType]
        (expr, paramsSchema)
      }
    }
  }

  def typedCheckChildTypeNested(dt: DataType): Unit = {
    if (dt.isInstanceOf[ArrayType] || dt.isInstanceOf[MapType] || dt.isInstanceOf[StructType]) {
      throw new NotImplementedError(
        s"Data type conversion not implemented for nesting type with child type: ${dt.simpleString}")
    }
  }

  private def arithDecimalReturnType(e: Expression): DataType = {
    import scala.math.max
    import scala.math.min
    e match {
      case Add(e1 @ DecimalType.Expression(p1, s1), e2 @ DecimalType.Expression(p2, s2)) =>
        val resultScale = max(s1, s2)
        if (SQLConf.get.decimalOperationsAllowPrecisionLoss) {
          DecimalType.adjustPrecisionScale(max(p1 - s1, p2 - s2) + resultScale + 1, resultScale)
        } else {
          DecimalType.bounded(max(p1 - s1, p2 - s2) + resultScale + 1, resultScale)
        }

      case Subtract(e1 @ DecimalType.Expression(p1, s1), e2 @ DecimalType.Expression(p2, s2)) =>
        val resultScale = max(s1, s2)
        if (SQLConf.get.decimalOperationsAllowPrecisionLoss) {
          DecimalType.adjustPrecisionScale(max(p1 - s1, p2 - s2) + resultScale + 1, resultScale)
        } else {
          DecimalType.bounded(max(p1 - s1, p2 - s2) + resultScale + 1, resultScale)
        }

      case Multiply(e1 @ DecimalType.Expression(p1, s1), e2 @ DecimalType.Expression(p2, s2)) =>
        if (SQLConf.get.decimalOperationsAllowPrecisionLoss) {
          DecimalType.adjustPrecisionScale(p1 + p2 + 1, s1 + s2)
        } else {
          DecimalType.bounded(p1 + p2 + 1, s1 + s2)
        }

      case Divide(e1 @ DecimalType.Expression(p1, s1), e2 @ DecimalType.Expression(p2, s2)) =>
        if (SQLConf.get.decimalOperationsAllowPrecisionLoss) {
          // Precision: p1 - s1 + s2 + max(6, s1 + p2 + 1)
          // Scale: max(6, s1 + p2 + 1)
          val intDig = p1 - s1 + s2
          val scale = max(DecimalType.MINIMUM_ADJUSTED_SCALE, s1 + p2 + 1)
          val prec = intDig + scale
          DecimalType.adjustPrecisionScale(prec, scale)
        } else {
          var intDig = min(DecimalType.MAX_SCALE, p1 - s1 + s2)
          var decDig = min(DecimalType.MAX_SCALE, max(6, s1 + p2 + 1))
          val diff = (intDig + decDig) - DecimalType.MAX_SCALE
          if (diff > 0) {
            decDig -= diff / 2 + 1
            intDig = DecimalType.MAX_SCALE - decDig
          }
          DecimalType.bounded(intDig + decDig, decDig)
        }

      case Remainder(e1 @ DecimalType.Expression(p1, s1), e2 @ DecimalType.Expression(p2, s2)) =>
        if (SQLConf.get.decimalOperationsAllowPrecisionLoss) {
          DecimalType.adjustPrecisionScale(min(p1 - s1, p2 - s2) + max(s1, s2), max(s1, s2))
        } else {
          DecimalType.bounded(min(p1 - s1, p2 - s2) + max(s1, s2), max(s1, s2))
        }

      case Pmod(e1 @ DecimalType.Expression(p1, s1), e2 @ DecimalType.Expression(p2, s2)) =>
        if (SQLConf.get.decimalOperationsAllowPrecisionLoss) {
          DecimalType.adjustPrecisionScale(min(p1 - s1, p2 - s2) + max(s1, s2), max(s1, s2))
        } else {
          DecimalType.bounded(min(p1 - s1, p2 - s2) + max(s1, s2), max(s1, s2))
        }
      case e => e.dataType
    }
  }
}
