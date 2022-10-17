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

import com.google.protobuf.ByteString
import org.apache.spark.sql.catalyst.expressions.Abs
import org.apache.spark.sql.catalyst.expressions.Acos
import org.apache.spark.sql.catalyst.expressions.Add
import org.apache.spark.sql.catalyst.expressions.And
import org.apache.spark.sql.catalyst.expressions.Asin
import org.apache.spark.sql.catalyst.expressions.Atan
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.CaseWhen
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.expressions.Ceil
import org.apache.spark.sql.catalyst.expressions.Coalesce
import org.apache.spark.sql.catalyst.expressions.Concat
import org.apache.spark.sql.catalyst.expressions.Cos
import org.apache.spark.sql.catalyst.expressions.Divide
import org.apache.spark.sql.catalyst.expressions.EqualTo
import org.apache.spark.sql.catalyst.expressions.Exp
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Floor
import org.apache.spark.sql.catalyst.expressions.GreaterThan
import org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual
import org.apache.spark.sql.catalyst.expressions.In
import org.apache.spark.sql.catalyst.expressions.InSet
import org.apache.spark.sql.catalyst.expressions.IsNotNull
import org.apache.spark.sql.catalyst.expressions.IsNull
import org.apache.spark.sql.catalyst.expressions.LessThan
import org.apache.spark.sql.catalyst.expressions.LessThanOrEqual
import org.apache.spark.sql.catalyst.expressions.Like
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.Log
import org.apache.spark.sql.catalyst.expressions.Log10
import org.apache.spark.sql.catalyst.expressions.Log2
import org.apache.spark.sql.catalyst.expressions.Lower
import org.apache.spark.sql.catalyst.expressions.Md5
import org.apache.spark.sql.catalyst.expressions.Multiply
import org.apache.spark.sql.catalyst.expressions.Not
import org.apache.spark.sql.catalyst.expressions.NullIf
import org.apache.spark.sql.catalyst.expressions.OctetLength
import org.apache.spark.sql.catalyst.expressions.Or
import org.apache.spark.sql.catalyst.expressions.Remainder
import org.apache.spark.sql.catalyst.expressions.Round
import org.apache.spark.sql.catalyst.expressions.Sha2
import org.apache.spark.sql.catalyst.expressions.Signum
import org.apache.spark.sql.catalyst.expressions.Sin
import org.apache.spark.sql.catalyst.expressions.Sqrt
import org.apache.spark.sql.catalyst.expressions.StartsWith
import org.apache.spark.sql.catalyst.expressions.StringTrim
import org.apache.spark.sql.catalyst.expressions.StringTrimLeft
import org.apache.spark.sql.catalyst.expressions.StringTrimRight
import org.apache.spark.sql.catalyst.expressions.Substring
import org.apache.spark.sql.catalyst.expressions.Subtract
import org.apache.spark.sql.catalyst.expressions.Tan
import org.apache.spark.sql.catalyst.expressions.TruncDate
import org.apache.spark.sql.catalyst.expressions.Upper
import org.apache.spark.sql.catalyst.expressions.aggregate.Average
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.expressions.aggregate.Max
import org.apache.spark.sql.catalyst.expressions.aggregate.Min
import org.apache.spark.sql.catalyst.expressions.aggregate.StddevPop
import org.apache.spark.sql.catalyst.expressions.aggregate.StddevSamp
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.catalyst.expressions.aggregate.VariancePop
import org.apache.spark.sql.catalyst.expressions.aggregate.VarianceSamp
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.catalyst.expressions.If
import org.apache.spark.sql.catalyst.expressions.MakeDecimal
import org.apache.spark.sql.catalyst.expressions.UnscaledValue
import org.apache.spark.sql.catalyst.plans.FullOuter
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.LeftAnti
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.catalyst.plans.LeftSemi
import org.apache.spark.sql.catalyst.plans.RightOuter
import org.apache.spark.sql.types.ArrayType
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
import org.apache.spark.SparkEnv
import org.blaze.{protobuf => pb}
import org.blaze.protobuf.PhysicalFallbackToJvmExprNode
import org.blaze.protobuf.ScalarFunction

object NativeConverters {
  def convertToScalarType(dt: DataType): pb.PrimitiveScalarType = {
    dt match {
      case NullType => pb.PrimitiveScalarType.NULL
      case BooleanType => pb.PrimitiveScalarType.BOOL
      case ByteType => pb.PrimitiveScalarType.INT8
      case ShortType => pb.PrimitiveScalarType.INT16
      case IntegerType => pb.PrimitiveScalarType.INT32
      case LongType => pb.PrimitiveScalarType.INT64
      case FloatType => pb.PrimitiveScalarType.FLOAT32
      case DoubleType => pb.PrimitiveScalarType.FLOAT64
      case StringType => pb.PrimitiveScalarType.UTF8
      case _: DecimalType => pb.PrimitiveScalarType.DECIMAL128
      case _: ArrayType => pb.PrimitiveScalarType.UTF8 // FIXME
      case _ => throw new NotImplementedError(s"convert $dt to DF scalar type not supported")
    }
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

      // decimal
      case t: DecimalType =>
        arrowTypeBuilder.setDECIMAL(
          org.blaze.protobuf.Decimal
            .newBuilder()
            .setWhole(t.precision)
            .setFractional(t.scale)
            .build())

      case _ =>
        throw new NotImplementedError(s"Data type conversion not implemented ${sparkDataType}")
    }
    arrowTypeBuilder.build()
  }

  def convertValue(sparkValue: Any, dataType: DataType): pb.ScalarValue = {
    val scalarValueBuilder = pb.ScalarValue.newBuilder()
    dataType match {
      case NullType => scalarValueBuilder.setNullValue(pb.PrimitiveScalarType.NULL)
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
          pb.ScalarDecimalValue
            .newBuilder()
            .setDecimal(decimalType)
            .setLongValue(decimalValue.toUnscaledLong))
      }
      // TODO: support complex data types
      case _: ArrayType | _: StructType | _: MapType if sparkValue == null =>
        pb.PrimitiveScalarType.NULL
      case _ => throw new NotImplementedError(s"Value conversion not implemented ${dataType}")
    }

    // TODO: support complex data types
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

  def convertExpr(sparkExpr: Expression, useAttrExprId: Boolean = true): pb.PhysicalExprNode = {
    def buildExprNode(buildFn: pb.PhysicalExprNode.Builder => pb.PhysicalExprNode.Builder)
        : pb.PhysicalExprNode =
      buildFn(pb.PhysicalExprNode.newBuilder()).build()

    def buildAggrExprNode(
        aggrFunction: pb.AggregateFunction,
        child: Expression): pb.PhysicalExprNode =
      buildExprNode {
        _.setAggregateExpr(
          pb.PhysicalAggregateExprNode
            .newBuilder()
            .setAggrFunction(aggrFunction)
            .setExpr(convertExpr(child, useAttrExprId)))
      }

    def buildBinaryExprNode(
        left: Expression,
        right: Expression,
        op: String): pb.PhysicalExprNode =
      buildExprNode {
        _.setBinaryExpr(
          pb.PhysicalBinaryExprNode
            .newBuilder()
            .setL(convertExpr(left, useAttrExprId))
            .setR(convertExpr(right, useAttrExprId))
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
            .addAllArgs(args.map(expr => convertExpr(expr, useAttrExprId)).asJava)
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
            .setFun(ScalarFunction.SparkExtFunctions)
            .addAllArgs(args.map(expr => convertExpr(expr, useAttrExprId)).asJava)
            .setReturnType(convertDataType(dataType)))
      }

    def unpackBinaryTypeCast(expr: Expression) =
      expr match {
        case Cast(inner, BinaryType, _) => inner
        case expr => expr
      }

    sparkExpr match {
      case l @ Literal(value, dataType) =>
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
                        pb.ScalarValue.newBuilder().setNullValue(convertToScalarType(NullType)))
                    }))
              case _ =>
                b.setLiteral(
                  pb.ScalarValue.newBuilder().setNullValue(convertToScalarType(dataType)))
            }
          } else {
            b.setLiteral(convertValue(value, dataType))
          }
        }
      case ar: AttributeReference =>
        buildExprNode {
          if (useAttrExprId) {
            _.setColumn(pb.PhysicalColumn.newBuilder().setName(s"#${ar.exprId.id}").build())
          } else {
            _.setColumn(pb.PhysicalColumn.newBuilder().setName(ar.name).build())
          }
        }

      // cast
      case Cast(child, dataType, _) =>
        buildExprNode {
          _.setTryCast(
            pb.PhysicalTryCastNode
              .newBuilder()
              .setExpr(convertExpr(child, useAttrExprId))
              .setArrowType(convertDataType(dataType))
              .build())
        }

      // in
      case In(value, list) =>
        // TODO:
        //  some types are not yet supported if datafusion (like Date32), keep
        //  this code until they are implemented
        val supportedTypes = Seq(
          FloatType,
          DoubleType,
          ByteType,
          ShortType,
          IntegerType,
          LongType,
          BooleanType,
          StringType)
        if (!supportedTypes.contains(value.dataType)) {
          throw new NotImplementedError(
            s"native In() does not support data type: ${value.dataType}")
        }

        buildExprNode {
          _.setInList(
            pb.PhysicalInListNode
              .newBuilder()
              .setExpr(convertExpr(value, useAttrExprId))
              .addAllList(list.map(expr => convertExpr(expr, useAttrExprId)).asJava))
        }

      // in
      case InSet(value, set) =>
        // TODO:
        //  some types are not yet supported if datafusion (like Date32), keep
        //  this code until they are implemented
        val supportedTypes = Seq(
          FloatType,
          DoubleType,
          ByteType,
          ShortType,
          IntegerType,
          LongType,
          BooleanType,
          StringType)
        if (!supportedTypes.contains(value.dataType)) {
          throw new NotImplementedError(
            s"native InSet() does not support data type: ${value.dataType}")
        }

        buildExprNode {
          _.setInList(
            pb.PhysicalInListNode
              .newBuilder()
              .setExpr(convertExpr(value, useAttrExprId))
              .addAllList(set.map {
                case utf8string: UTF8String =>
                  convertExpr(Literal(utf8string, StringType), useAttrExprId)
                case v => convertExpr(Literal.apply(v), useAttrExprId)
              }.asJava))
        }

      // unary ops
      case IsNull(child) =>
        buildExprNode {
          _.setIsNullExpr(
            pb.PhysicalIsNull.newBuilder().setExpr(convertExpr(child, useAttrExprId)).build())
        }
      case IsNotNull(child) =>
        buildExprNode {
          _.setIsNotNullExpr(
            pb.PhysicalIsNotNull.newBuilder().setExpr(convertExpr(child, useAttrExprId)).build())
        }
      case Not(EqualTo(lhs, rhs)) => buildBinaryExprNode(lhs, rhs, "NotEq")
      case Not(child) =>
        buildExprNode {
          _.setNotExpr(
            pb.PhysicalNot.newBuilder().setExpr(convertExpr(child, useAttrExprId)).build())
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
      case Divide(lhs, rhs) =>
        buildBinaryExprNode(lhs, new NullIf(rhs, Literal.create(0, rhs.dataType)), "Divide")
      case Remainder(lhs, rhs) =>
        buildBinaryExprNode(lhs, new NullIf(rhs, Literal.create(0, rhs.dataType)), "Modulo")
      case Like(lhs, rhs) => buildBinaryExprNode(lhs, rhs, "Like")
      case And(lhs, rhs) => buildBinaryExprNode(lhs, rhs, "And")
      case Or(lhs, rhs) => buildBinaryExprNode(lhs, rhs, "Or")

      // builtin scalar functions
      case e: Sqrt => buildScalarFunction(pb.ScalarFunction.Sqrt, e.children, e.dataType)
      case e: Sin => buildScalarFunction(pb.ScalarFunction.Sin, e.children, e.dataType)
      case e: Cos => buildScalarFunction(pb.ScalarFunction.Cos, e.children, e.dataType)
      case e: Tan => buildScalarFunction(pb.ScalarFunction.Tan, e.children, e.dataType)
      case e: Asin => buildScalarFunction(pb.ScalarFunction.Asin, e.children, e.dataType)
      case e: Acos => buildScalarFunction(pb.ScalarFunction.Acos, e.children, e.dataType)
      case e: Atan => buildScalarFunction(pb.ScalarFunction.Atan, e.children, e.dataType)
      case e: Exp => buildScalarFunction(pb.ScalarFunction.Exp, e.children, e.dataType)
      case e: Log => buildScalarFunction(pb.ScalarFunction.Log, e.children, e.dataType)
      case e: Log2 => buildScalarFunction(pb.ScalarFunction.Log2, e.children, e.dataType)
      case e: Log10 => buildScalarFunction(pb.ScalarFunction.Log10, e.children, e.dataType)
      case e: Floor =>
        buildExprNode {
          _.setTryCast(
            pb.PhysicalTryCastNode
              .newBuilder()
              .setExpr(buildScalarFunction(pb.ScalarFunction.Floor, e.children, e.dataType))
              .setArrowType(convertDataType(e.dataType))
              .build())
        }
      case e: Ceil =>
        buildExprNode {
          _.setTryCast(
            pb.PhysicalTryCastNode
              .newBuilder()
              .setExpr(buildScalarFunction(pb.ScalarFunction.Ceil, e.children, e.dataType))
              .setArrowType(convertDataType(e.dataType))
              .build())
        }
      case Round(_1, Literal(0, _)) =>
        buildScalarFunction(pb.ScalarFunction.Round, Seq(_1), _1.dataType)
      // case Nothing => buildScalarFunction(pb.ScalarFunction.TRUNC, Nil)
      case e: Abs => buildScalarFunction(pb.ScalarFunction.Abs, e.children, e.dataType)
      case e: Signum => buildScalarFunction(pb.ScalarFunction.Signum, e.children, e.dataType)
      case e: OctetLength =>
        buildScalarFunction(pb.ScalarFunction.OctetLength, e.children, e.dataType)
      case e: Concat => buildScalarFunction(pb.ScalarFunction.Concat, e.children, e.dataType)
      case e: Lower => buildScalarFunction(pb.ScalarFunction.Lower, e.children, e.dataType)
      case e: Upper => buildScalarFunction(pb.ScalarFunction.Upper, e.children, e.dataType)
      case e: StringTrim =>
        buildScalarFunction(pb.ScalarFunction.Trim, e.srcStr +: e.trimStr.toSeq, e.dataType)
      case e: StringTrimLeft =>
        buildScalarFunction(pb.ScalarFunction.Ltrim, e.srcStr +: e.trimStr.toSeq, e.dataType)
      case e: StringTrimRight =>
        buildScalarFunction(pb.ScalarFunction.Rtrim, e.srcStr +: e.trimStr.toSeq, e.dataType)
      // case Nothing => buildScalarFunction(pb.ScalarFunction.TOTIMESTAMP, Nil)
      // case Nothing => buildScalarFunction(pb.ScalarFunction.ARRAY, Nil)
      case e: NullIf => buildScalarFunction(pb.ScalarFunction.NullIf, e.children, e.dataType)
      // case e: DatePart => buildScalarFunction(pb.ScalarFunction.DatePart, e.children, e.dataType)
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
      // case Nothing => buildScalarFunction(pb.ScalarFunction.TOTIMESTAMPMILLIS, Nil)
      case StartsWith(_1, _2) =>
        buildScalarFunction(pb.ScalarFunction.StartsWith, Seq(_1, _2), BooleanType)
      case e: Substring =>
        val newChildren = e.children.head +: e.children.tail.map {
          case c if c.dataType != LongType => Cast(c, LongType)
          case c => c
        }
        buildScalarFunction(pb.ScalarFunction.Substr, newChildren, e.dataType)
      case e: Coalesce => buildScalarFunction(pb.ScalarFunction.Coalesce, e.children, e.dataType)

      case If(predicate, trueValue, falseValue) =>
        buildExprNode {
          _.setCase(
            pb.PhysicalCaseNode
              .newBuilder()
              .addWhenThenExpr(
                pb.PhysicalWhenThen
                  .newBuilder()
                  .setWhenExpr(convertExpr(predicate, useAttrExprId))
                  .setThenExpr(convertExpr(trueValue, useAttrExprId)))
              .setElseExpr(convertExpr(falseValue, useAttrExprId)))
        }
      case CaseWhen(branches, elseValue) =>
        val caseExpr = pb.PhysicalCaseNode.newBuilder()
        val whenThens = branches.map {
          case (w, t) =>
            val whenThen = pb.PhysicalWhenThen.newBuilder()
            whenThen.setWhenExpr(convertExpr(w, useAttrExprId))
            whenThen.setThenExpr(convertExpr(t, useAttrExprId))
            whenThen.build()
        }
        caseExpr.addAllWhenThenExpr(whenThens.asJava)
        elseValue.foreach(el => caseExpr.setElseExpr(convertExpr(el, useAttrExprId)))
        pb.PhysicalExprNode.newBuilder().setCase(caseExpr).build()

      // aggr bypass
      case UnscaledValue(_1) =>
        buildExtScalarFunction("UnscaledValue", Seq(_1), LongType)
      case MakeDecimal(_1, precision, scale) =>
        val args = Seq(_1, Literal(precision), Literal(scale))
        buildExtScalarFunction("MakeDecimal", args, DecimalType(precision, scale))

      // aggr
      case Min(_1) => buildAggrExprNode(pb.AggregateFunction.MIN, _1)
      case Max(_1) => buildAggrExprNode(pb.AggregateFunction.MAX, _1)
      case Sum(_1) => buildAggrExprNode(pb.AggregateFunction.SUM, _1)
      case Average(_1) => buildAggrExprNode(pb.AggregateFunction.AVG, _1)
      case Count(Seq(_1)) => buildAggrExprNode(pb.AggregateFunction.COUNT, _1)
      case Count(_n) if !_n.exists(_.nullable) =>
        buildAggrExprNode(pb.AggregateFunction.COUNT, Literal(1))
      case VarianceSamp(_1) => buildAggrExprNode(pb.AggregateFunction.VARIANCE, _1)
      case VariancePop(_1) => buildAggrExprNode(pb.AggregateFunction.VARIANCE_POP, _1)
      case StddevSamp(_1) => buildAggrExprNode(pb.AggregateFunction.STDDEV, _1)
      case StddevPop(_1) => buildAggrExprNode(pb.AggregateFunction.STDDEV_POP, _1)

      // hive UDF
      case e
          if Seq(
            "org.apache.spark.sql.hive.HiveSimpleUDF",
            "org.apache.spark.sql.hive.HiveGenericUDF").contains(e.getClass.getName) =>
        assert(
          SparkEnv.get.conf.getBoolean("spark.blaze.udf.enabled", defaultValue = false),
          "stop converting UDF because spark.blaze.udf.enabled = false")
        val bounded = e.withNewChildren(e.children.zipWithIndex.map {
          case (param, index) => BoundReference(index, param.dataType, param.nullable)
        })
        val serialized = serializeExpression(bounded.asInstanceOf[Expression with Serializable])

        pb.PhysicalExprNode
          .newBuilder()
          .setFallbackToJvmExpr(
            PhysicalFallbackToJvmExprNode
              .newBuilder()
              .setSerialized(ByteString.copyFrom(serialized))
              .setReturnType(convertDataType(bounded.dataType))
              .setReturnNullable(bounded.nullable)
              .addAllParams(e.children.map(expr => convertExpr(expr, useAttrExprId)).asJava))
          .build()

      case unsupportedExpression =>
        throw new NotImplementedError(
          s"unsupported exception: ${unsupportedExpression} (${unsupportedExpression.getClass.getName})")
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

  def serializeExpression(udf: Expression with Serializable): Array[Byte] = {
    Utils.tryWithResource(new ByteArrayOutputStream()) { bos =>
      Utils.tryWithResource(new ObjectOutputStream(bos)) { oos =>
        oos.writeObject(udf)
        null
      }
      bos.toByteArray
    }
  }

  def deserializeExpression(serialized: Array[Byte]): Expression with Serializable = {
    Utils.tryWithResource(new ByteArrayInputStream(serialized)) { bis =>
      Utils.tryWithResource(new ObjectInputStream(bis)) { ois =>
        ois.readObject().asInstanceOf[Expression with Serializable]
      }
    }
  }
}
