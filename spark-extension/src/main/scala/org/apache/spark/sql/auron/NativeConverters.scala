/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.auron

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.math.max
import scala.math.min

import com.google.protobuf.ByteString
import org.apache.arrow.c.CDataDictionaryProvider
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.commons.lang3.reflect.FieldUtils
import org.apache.commons.lang3.reflect.MethodUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.auron.util.Using
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Abs, Acos, Add, Alias, And, Asin, Atan, Attribute, AttributeReference, BitwiseAnd, BitwiseOr, BoundReference, CaseWhen, Cast, Ceil, CheckOverflow, Coalesce, Concat, ConcatWs, Contains, Cos, CreateArray, CreateNamedStruct, DayOfMonth, Divide, EndsWith, EqualTo, Exp, Expression, Floor, GetArrayItem, GetJsonObject, GetMapValue, GetStructField, GreaterThan, GreaterThanOrEqual, If, In, InSet, IsNotNull, IsNull, LeafExpression, Length, LessThan, LessThanOrEqual, Like, Literal, Log, Log10, Log2, Lower, MakeDecimal, Md5, Month, Multiply, Murmur3Hash, Not, NullIf, OctetLength, Or, Remainder, Sha2, ShiftLeft, ShiftRight, Signum, Sin, Sqrt, StartsWith, StringRepeat, StringSpace, StringTrim, StringTrimLeft, StringTrimRight, Substring, Subtract, Tan, Unevaluable, UnscaledValue, Upper, XxHash64, Year}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, Average, CollectList, CollectSet, Count, DeclarativeAggregate, First, Max, Min, Sum, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.expressions.codegen.ExprCode
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.catalyst.plans.ExistenceJoin
import org.apache.spark.sql.catalyst.plans.FullOuter
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.LeftAnti
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.catalyst.plans.LeftSemi
import org.apache.spark.sql.catalyst.plans.RightOuter
import org.apache.spark.sql.execution.ExecSubqueryExpression
import org.apache.spark.sql.execution.auron.arrowio.util.ArrowUtils
import org.apache.spark.sql.execution.auron.arrowio.util.ArrowUtils.ROOT_ALLOCATOR
import org.apache.spark.sql.execution.auron.arrowio.util.ArrowWriter
import org.apache.spark.sql.execution.auron.plan.Util
import org.apache.spark.sql.hive.auron.HiveUDFUtil
import org.apache.spark.sql.hive.auron.HiveUDFUtil.getFunctionClassName
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.AtomicType
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DateType
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

import org.apache.auron.{protobuf => pb}
import org.apache.auron.protobuf.PhysicalExprNode

object NativeConverters extends Logging {
  def udfJsonEnabled: Boolean = AuronConf.UDF_JSON_ENABLED.booleanConf()
  def udfBrickHouseEnabled: Boolean = AuronConf.UDF_BRICKHOUSE_ENABLED.booleanConf()
  def decimalArithOpEnabled: Boolean = AuronConf.DECIMAL_ARITH_OP_ENABLED.booleanConf()

  def scalarTypeSupported(dataType: DataType): Boolean = {
    dataType match {
      case NullType | BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType |
          DoubleType | StringType | DateType | TimestampType =>
        true
      case t: DecimalType if DecimalType.is64BitDecimalType(t) => true
      case _: DecimalType =>
        // auron only supports 64-bit decimal
        false
      case at: ArrayType => scalarTypeSupported(at.elementType)
      case m: MapType =>
        scalarTypeSupported(m.keyType) && scalarTypeSupported(m.valueType)
      case s: StructType =>
        s.fields.forall(e => scalarTypeSupported(e.dataType))
      case _ => false
    }
  }

  def roundRobinTypeSupported(dataType: DataType): Boolean = dataType match {
    case MapType(_, _, _) => false
    case ArrayType(elementType, _) => roundRobinTypeSupported(elementType)
    case StructType(fields) => fields.forall(f => roundRobinTypeSupported(f.dataType))
    case _ => true
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
          org.apache.auron.protobuf.Decimal
            .newBuilder()
            .setWhole(Math.max(t.precision, 1))
            .setFractional(t.scale)
            .build())

      // array/list
      case a: ArrayType =>
        arrowTypeBuilder.setLIST(
          org.apache.auron.protobuf.List
            .newBuilder()
            .setFieldType(
              pb.Field
                .newBuilder()
                .setName("item")
                .setArrowType(convertDataType(a.elementType))
                .setNullable(a.containsNull))
            .build())

      case m: MapType =>
        arrowTypeBuilder.setMAP(
          org.apache.auron.protobuf.Map
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
        arrowTypeBuilder.setSTRUCT(
          org.apache.auron.protobuf.Struct
            .newBuilder()
            .addAllSubFieldTypes(
              s.fields
                .map(e =>
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

  def convertJoinFilter(
      filterExpr: Expression,
      leftOutput: Seq[Attribute],
      rightOutput: Seq[Attribute]): pb.JoinFilter = {
    val schema = filterExpr.references.toSeq
    val columnIndices = mutable.ArrayBuffer[pb.ColumnIndex]()
    for (attr <- schema) {
      attr.exprId match {
        case exprId if leftOutput.exists(_.exprId == exprId) =>
          columnIndices += pb.ColumnIndex
            .newBuilder()
            .setSide(pb.JoinSide.LEFT_SIDE)
            .setIndex(leftOutput.indexWhere(_.exprId == attr.exprId))
            .build()
        case exprId if rightOutput.exists(_.exprId == exprId) =>
          columnIndices += pb.ColumnIndex
            .newBuilder()
            .setSide(pb.JoinSide.RIGHT_SIDE)
            .setIndex(rightOutput.indexWhere(_.exprId == attr.exprId))
            .build()
        case _ =>
          columnIndices += pb.ColumnIndex.newBuilder().buildPartial()
      }
    }
    pb.JoinFilter
      .newBuilder()
      .setExpression(convertExpr(filterExpr))
      .setSchema(Util.getNativeSchema(schema))
      .addAllColumnIndices(columnIndices.asJava)
      .build()
  }

  abstract class NativeExprWrapperBase(
      _wrapped: pb.PhysicalExprNode,
      override val dataType: DataType = NullType,
      override val nullable: Boolean = true,
      override val children: Seq[Expression] = Nil)
      extends Unevaluable {
    val wrapped: PhysicalExprNode = _wrapped

    override def toString(): String = s"$getClass() dataType:$dataType)"
  }

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
              Shims.get.createNativeExprWrapper(converted, child.dataType, child.nullable)
            } catch {
              case _: NotImplementedError =>
                val fallbacked = convertExpr(child)
                Shims.get.createNativeExprWrapper(fallbacked, child.dataType, child.nullable)
            }
          }
          convertExprWithFallback(childrenConverted, isPruningExpr = false, fallbackToError)
        case _ =>
          fallbackToError(sparkExpr)
      }

    } catch {
      case e: NotImplementedError =>
        logWarning(s"Falling back expression: $e")

        // update subquery result if needed
        sparkExpr.foreach {
          case subquery: ExecSubqueryExpression =>
            prepareExecSubquery(subquery)
          case _ =>
        }
        val exprString = sparkExpr.toString()

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
              .addAllParams(convertedChildren.keys.asJava)
              .setExprString(exprString))
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

  def buildExprNode(buildFn: pb.PhysicalExprNode.Builder => pb.PhysicalExprNode.Builder)
      : pb.PhysicalExprNode = {
    buildFn(pb.PhysicalExprNode.newBuilder()).build()
  }

  def convertExprWithFallback(
      sparkExpr: Expression,
      isPruningExpr: Boolean,
      fallback: Expression => pb.PhysicalExprNode): pb.PhysicalExprNode = {

    val buildBinaryExprNode: (Expression, Expression, String) => PhysicalExprNode =
      this.buildBinaryExprNode(_, _, _, isPruningExpr, fallback)
    val buildScalarFunction: (pb.ScalarFunction, Seq[Expression], DataType) => PhysicalExprNode =
      this.buildScalarFunctionNode(_, _, _, isPruningExpr, fallback)
    val buildExtScalarFunction: (String, Seq[Expression], DataType) => PhysicalExprNode =
      this.buildExtScalarFunctionNode(_, _, _, isPruningExpr, fallback)

    sparkExpr match {
      case e: NativeExprWrapperBase => e.wrapped
      case e: Literal =>
        val schema = StructType(Seq(StructField("", e.dataType, e.nullable)))
        val row = InternalRow(e.eval(null))
        Using.resource(
          VectorSchemaRoot.create(ArrowUtils.toArrowSchema(schema), ROOT_ALLOCATOR)) { root =>
          val arrowWriter = ArrowWriter.create(root)
          arrowWriter.write(row)
          arrowWriter.finish()

          val dictionaryProvider = new CDataDictionaryProvider()
          val bo = new ByteArrayOutputStream()
          Using(new ArrowStreamWriter(root, dictionaryProvider, bo)) { ipcWriter =>
            ipcWriter.start()
            ipcWriter.writeBatch()
            ipcWriter.end()
          }
          val ipcBytes = bo.toByteArray
          pb.PhysicalExprNode
            .newBuilder()
            .setLiteral(pb.ScalarValue.newBuilder().setIpcBytes(ByteString.copyFrom(ipcBytes)))
            .build()
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
      case subquery: ExecSubqueryExpression =>
        prepareExecSubquery(subquery)
        val serialized = serializeExpression(
          subquery.asInstanceOf[Expression with Serializable],
          StructType(Nil))
        buildExprNode {
          _.setSparkScalarSubqueryWrapperExpr(
            pb.PhysicalSparkScalarSubqueryWrapperExprNode
              .newBuilder()
              .setSerialized(ByteString.copyFrom(serialized))
              .setReturnType(convertDataType(subquery.dataType))
              .setReturnNullable(subquery.nullable))
        }

      // cast
      // not performing native cast for timestamp/dates (will use UDFWrapper instead)
      case cast: Cast
          if !Seq(cast.dataType, cast.child.dataType).exists(t =>
            t.isInstanceOf[TimestampType] || t.isInstanceOf[DateType]) =>
        buildExprNode {
          _.setTryCast(
            pb.PhysicalTryCastNode
              .newBuilder()
              .setExpr(convertExprWithFallback(cast.child, isPruningExpr, fallback))
              .setArrowType(convertDataType(cast.dataType))
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
      case InSet(value, set) =>
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
                case v => convertExpr(Literal.apply(v))
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

      case e: Add if !e.dataType.isInstanceOf[DecimalType] || decimalArithOpEnabled =>
        val lhs = e.left
        val rhs = e.right
        if (e.dataType.isInstanceOf[DecimalType]) {
          def resultDecimalType(p1: Int, s1: Int, p2: Int, s2: Int): DecimalType = {
            val resultScale = max(s1, s2)
            val resultPrecision = max(p1 - s1, p2 - s2) + resultScale + 1
            if (SQLConf.get.decimalOperationsAllowPrecisionLoss) {
              DecimalType.adjustPrecisionScale(resultPrecision, resultScale)
            } else {
              DecimalType.bounded(resultPrecision, resultScale)
            }
          }
          val resultType = (lhs.dataType, rhs.dataType) match {
            case (lhsType: DecimalType, rhsType: DecimalType) =>
              resultDecimalType(
                lhsType.precision,
                lhsType.scale,
                rhsType.precision,
                rhsType.scale)
          }

          buildExprNode {
            _.setCast(pb.PhysicalCastNode
              .newBuilder()
              .setArrowType(convertDataType(resultType))
              .setExpr(buildExprNode {
                _.setBinaryExpr(
                  pb.PhysicalBinaryExprNode
                    .newBuilder()
                    .setL(convertExprWithFallback(Cast(lhs, resultType), isPruningExpr, fallback))
                    .setR(convertExprWithFallback(rhs, isPruningExpr, fallback))
                    .setOp("Plus"))
              }))
          }
        } else {
          buildBinaryExprNode(lhs, rhs, "Plus")
        }

      case e: Subtract if !e.dataType.isInstanceOf[DecimalType] || decimalArithOpEnabled =>
        val lhs = e.left
        val rhs = e.right
        if (e.dataType.isInstanceOf[DecimalType]) {
          // copied from spark3.5
          def resultDecimalType(p1: Int, s1: Int, p2: Int, s2: Int): DecimalType = {
            val resultScale = max(s1, s2)
            val resultPrecision = max(p1 - s1, p2 - s2) + resultScale + 1
            if (SQLConf.get.decimalOperationsAllowPrecisionLoss) {
              DecimalType.adjustPrecisionScale(resultPrecision, resultScale)
            } else {
              DecimalType.bounded(resultPrecision, resultScale)
            }
          }
          val resultType = (lhs.dataType, rhs.dataType) match {
            case (lhsType: DecimalType, rhsType: DecimalType) =>
              resultDecimalType(
                lhsType.precision,
                lhsType.scale,
                rhsType.precision,
                rhsType.scale)
          }

          buildExprNode {
            _.setCast(pb.PhysicalCastNode
              .newBuilder()
              .setArrowType(convertDataType(resultType))
              .setExpr(buildExprNode {
                _.setBinaryExpr(
                  pb.PhysicalBinaryExprNode
                    .newBuilder()
                    .setL(convertExprWithFallback(Cast(lhs, resultType), isPruningExpr, fallback))
                    .setR(convertExprWithFallback(rhs, isPruningExpr, fallback))
                    .setOp("Minus"))
              }))
          }
        } else {
          buildBinaryExprNode(lhs, rhs, "Minus")
        }

      case e: Multiply if !e.dataType.isInstanceOf[DecimalType] || decimalArithOpEnabled =>
        val lhs = e.left
        val rhs = e.right
        if (e.dataType.isInstanceOf[DecimalType]) {
          // copied from spark3.5
          def resultDecimalType(p1: Int, s1: Int, p2: Int, s2: Int): DecimalType = {
            val resultScale = s1 + s2
            val resultPrecision = p1 + p2 + 1
            if (SQLConf.get.decimalOperationsAllowPrecisionLoss) {
              DecimalType.adjustPrecisionScale(resultPrecision, resultScale)
            } else {
              DecimalType.bounded(resultPrecision, resultScale)
            }
          }
          val resultType = (lhs.dataType, rhs.dataType) match {
            case (lhsType: DecimalType, rhsType: DecimalType) =>
              resultDecimalType(
                lhsType.precision,
                lhsType.scale,
                rhsType.precision,
                rhsType.scale)
          }

          buildExprNode {
            _.setCast(pb.PhysicalCastNode
              .newBuilder()
              .setArrowType(convertDataType(resultType))
              .setExpr(buildExprNode {
                _.setBinaryExpr(
                  pb.PhysicalBinaryExprNode
                    .newBuilder()
                    .setL(convertExprWithFallback(Cast(lhs, resultType), isPruningExpr, fallback))
                    .setR(convertExprWithFallback(rhs, isPruningExpr, fallback))
                    .setOp("Multiply"))
              }))
          }
        } else {
          buildBinaryExprNode(lhs, rhs, "Multiply")
        }

      case e: Divide if !e.dataType.isInstanceOf[DecimalType] || decimalArithOpEnabled =>
        val lhs = e.left
        val rhs = e.right
        if (e.dataType.isInstanceOf[DecimalType]) {
          // copied from spark3.5
          def resultDecimalType(p1: Int, s1: Int, p2: Int, s2: Int): DecimalType = {
            if (SQLConf.get.decimalOperationsAllowPrecisionLoss) {
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
          }
          val resultType = (lhs.dataType, rhs.dataType) match {
            case (lhsType: DecimalType, rhsType: DecimalType) =>
              resultDecimalType(
                lhsType.precision,
                lhsType.scale,
                rhsType.precision,
                rhsType.scale)
          }

          buildExprNode {
            _.setCast(pb.PhysicalCastNode
              .newBuilder()
              .setArrowType(convertDataType(resultType))
              .setExpr(buildExprNode {
                _.setBinaryExpr(
                  pb.PhysicalBinaryExprNode
                    .newBuilder()
                    .setL(convertExprWithFallback(Cast(lhs, resultType), isPruningExpr, fallback))
                    .setR(buildExtScalarFunction("NullIfZero", rhs :: Nil, rhs.dataType))
                    .setOp("Divide"))
              }))
          }
        } else {
          val resultType = e.dataType
          val lhsCasted = castIfNecessary(lhs, resultType)
          val rhsCasted = castIfNecessary(rhs, resultType)
          buildExprNode {
            _.setBinaryExpr(
              pb.PhysicalBinaryExprNode
                .newBuilder()
                .setL(convertExprWithFallback(lhsCasted, isPruningExpr, fallback))
                .setR(buildExtScalarFunction("NullIfZero", rhsCasted :: Nil, rhs.dataType))
                .setOp("Divide"))
          }
        }

      case e: Remainder =>
        val lhs = e.left
        val rhs = e.right
        val resultType = e.dataType
        rhs match {
          case rhs: Literal if rhs == Literal.default(rhs.dataType) =>
            convertExpr(Literal(null, e.dataType))
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
        assert(Shims.get.getLikeEscapeChar(e) == '\\')
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
      case And(lhs, rhs) if rhs.find(HiveUDFUtil.isHiveUDF).isDefined =>
        buildExprNode {
          _.setScAndExpr(
            pb.PhysicalSCAndExprNode
              .newBuilder()
              .setLeft(convertExprWithFallback(lhs, isPruningExpr, fallback))
              .setRight(convertExprWithFallback(rhs, isPruningExpr, fallback)))
        }
      case Or(lhs, rhs) if rhs.find(HiveUDFUtil.isHiveUDF).isDefined =>
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
      case e: Log =>
        buildScalarFunction(pb.ScalarFunction.Ln, e.children.map(nullIfNegative), e.dataType)
      case e: Log2 =>
        buildScalarFunction(pb.ScalarFunction.Log2, e.children.map(nullIfNegative), e.dataType)
      case e: Log10 =>
        buildScalarFunction(pb.ScalarFunction.Log10, e.children.map(nullIfNegative), e.dataType)
      case e: Floor if !e.dataType.isInstanceOf[DecimalType] =>
        if (e.child.dataType.isInstanceOf[LongType]) {
          convertExprWithFallback(e.child, isPruningExpr, fallback)
        } else {
          buildExprNode {
            _.setTryCast(
              pb.PhysicalTryCastNode
                .newBuilder()
                .setExpr(buildScalarFunction(pb.ScalarFunction.Floor, e.children, e.dataType))
                .setArrowType(convertDataType(e.dataType))
                .build())
          }
        }
      case e: Ceil if !e.dataType.isInstanceOf[DecimalType] =>
        if (e.child.dataType.isInstanceOf[LongType]) {
          convertExprWithFallback(e.child, isPruningExpr, fallback)
        } else {
          buildExprNode {
            _.setTryCast(
              pb.PhysicalTryCastNode
                .newBuilder()
                .setExpr(buildScalarFunction(pb.ScalarFunction.Ceil, e.children, e.dataType))
                .setArrowType(convertDataType(e.dataType))
                .build())
          }
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

      case e: Lower if AuronConf.CASE_CONVERT_FUNCTIONS_ENABLE.booleanConf() =>
        buildExtScalarFunction("StringLower", e.children, e.dataType)
      case e: Upper if AuronConf.CASE_CONVERT_FUNCTIONS_ENABLE.booleanConf() =>
        buildExtScalarFunction("StringUpper", e.children, e.dataType)

      case e: StringTrim =>
        buildScalarFunction(pb.ScalarFunction.Trim, e.srcStr +: e.trimStr.toSeq, e.dataType)
      case e: StringTrimLeft =>
        buildScalarFunction(pb.ScalarFunction.Ltrim, e.srcStr +: e.trimStr.toSeq, e.dataType)
      case e: StringTrimRight =>
        buildScalarFunction(pb.ScalarFunction.Rtrim, e.srcStr +: e.trimStr.toSeq, e.dataType)
      case e @ NullIf(left, right, _) =>
        buildExtScalarFunction("NullIf", left :: right :: Nil, e.dataType)
      case Md5(_1) =>
        buildScalarFunction(pb.ScalarFunction.MD5, Seq(unpackBinaryTypeCast(_1)), StringType)
      case Sha2(_1, Literal(224, _)) =>
        buildExtScalarFunction("Sha224", Seq(unpackBinaryTypeCast(_1)), StringType)
      case Sha2(_1, Literal(0, _)) =>
        buildExtScalarFunction("Sha256", Seq(unpackBinaryTypeCast(_1)), StringType)
      case Sha2(_1, Literal(256, _)) =>
        buildExtScalarFunction("Sha256", Seq(unpackBinaryTypeCast(_1)), StringType)
      case Sha2(_1, Literal(384, _)) =>
        buildExtScalarFunction("Sha384", Seq(unpackBinaryTypeCast(_1)), StringType)
      case Sha2(_1, Literal(512, _)) =>
        buildExtScalarFunction("Sha512", Seq(unpackBinaryTypeCast(_1)), StringType)
      case Murmur3Hash(children, 42) =>
        buildExtScalarFunction("Murmur3Hash", children, IntegerType)
      case XxHash64(children, 42L) =>
        buildExtScalarFunction("XxHash64", children, LongType)

      case Year(child) => buildExtScalarFunction("Year", child :: Nil, IntegerType)
      case Month(child) => buildExtScalarFunction("Month", child :: Nil, IntegerType)
      case DayOfMonth(child) => buildExtScalarFunction("Day", child :: Nil, IntegerType)

      // startswith is converted to scalar function in pruning-expr mode
      case StartsWith(expr, Literal(prefix, StringType)) if isPruningExpr =>
        buildExprNode(
          _.setScalarFunction(
            pb.PhysicalScalarFunctionNode
              .newBuilder()
              .setName("starts_with")
              .setFun(pb.ScalarFunction.StartsWith)
              .addArgs(convertExprWithFallback(expr, isPruningExpr, fallback))
              .addArgs(convertExpr(Literal(prefix, StringType)))
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
          if pos.asInstanceOf[Int] > 0 && len.asInstanceOf[Int] >= 0 =>
        val longPos = pos.asInstanceOf[Int].toLong
        val longLen = len.asInstanceOf[Int].toLong
        buildScalarFunction(
          pb.ScalarFunction.Substr,
          str :: Literal(longPos) :: Literal(longLen) :: Nil,
          StringType)

      case StringSpace(n) =>
        buildExtScalarFunction("StringSpace", n :: Nil, StringType)

      case StringRepeat(str, n @ Literal(_, IntegerType)) =>
        buildExtScalarFunction("StringRepeat", str :: n :: Nil, StringType)

      case e: Concat if e.children.forall(_.dataType == StringType) =>
        buildExtScalarFunction("StringConcat", e.children, e.dataType)

      case e: ConcatWs
          if e.children.nonEmpty
            && e.children.head.isInstanceOf[Literal]
            && e.children.forall(c =>
              c.dataType == StringType || c.dataType == ArrayType(StringType)) =>
        buildExtScalarFunction("StringConcatWs", e.children, e.dataType)

      case e: Coalesce =>
        val children = e.children.map(Cast(_, e.dataType))
        buildScalarFunction(pb.ScalarFunction.Coalesce, children, e.dataType)

      case e @ If(predicate, trueValue, falseValue) =>
        val castedTrueValue = trueValue match {
          case t if t.dataType != e.dataType => Cast(t, e.dataType)
          case t => t
        }
        val castedFalseValue = falseValue match {
          case f if f.dataType != e.dataType => Cast(f, e.dataType)
          case f => f
        }
        val caseWhen = CaseWhen(Seq((predicate, castedTrueValue)), castedFalseValue)
        convertExprWithFallback(caseWhen, isPruningExpr, fallback)

      case e @ CaseWhen(branches, elseValue) =>
        val caseExpr = pb.PhysicalCaseNode.newBuilder()
        val whenThens = branches.map { case (w, t) =>
          val casted = t match {
            case t if t.dataType != e.dataType => Cast(t, e.dataType)
            case t => t
          }
          pb.PhysicalWhenThen
            .newBuilder()
            .setWhenExpr(convertExprWithFallback(w, isPruningExpr, fallback))
            .setThenExpr(convertExprWithFallback(casted, isPruningExpr, fallback))
            .build()
        }
        caseExpr.addAllWhenThenExpr(whenThens.asJava)
        elseValue.foreach { el =>
          val casted = el match {
            case el if el.dataType != e.dataType => Cast(el, e.dataType)
            case el => el
          }
          caseExpr.setElseExpr(convertExprWithFallback(casted, isPruningExpr, fallback))
        }
        pb.PhysicalExprNode.newBuilder().setCase(caseExpr).build()

      // expressions for DecimalPrecision rule
      case UnscaledValue(_1) if decimalArithOpEnabled =>
        val args = _1 :: Nil
        buildExtScalarFunction("UnscaledValue", args, LongType)

      case e: MakeDecimal if decimalArithOpEnabled =>
        val precision = e.precision
        val scale = e.scale
        val args =
          e.child :: Literal
            .apply(precision, IntegerType) :: Literal.apply(scale, IntegerType) :: Nil
        buildExtScalarFunction("MakeDecimal", args, DecimalType(precision, scale))

      case e: CheckOverflow if decimalArithOpEnabled =>
        val precision = e.dataType.precision
        val scale = e.dataType.scale
        val args =
          e.child :: Literal
            .apply(precision, IntegerType) :: Literal.apply(scale, IntegerType) :: Nil
        buildExtScalarFunction("CheckOverflow", args, DecimalType(precision, scale))

      case e: NormalizeNaNAndZero
          if e.dataType.isInstanceOf[FloatType] || e.dataType.isInstanceOf[DoubleType] =>
        buildExtScalarFunction("NormalizeNanAndZero", e.children, e.dataType)

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

      case e: GetArrayItem
          if e.ordinal.isInstanceOf[Literal] && e.ordinal
            .asInstanceOf[Literal]
            .value
            .isInstanceOf[Number] =>
        // NOTE: data-fusion index starts from 1
        val ordinalValue = e.ordinal.asInstanceOf[Literal].value.asInstanceOf[Number]
        buildExprNode {
          _.setGetIndexedFieldExpr(
            pb.PhysicalGetIndexedFieldExprNode
              .newBuilder()
              .setExpr(convertExprWithFallback(e.child, isPruningExpr, fallback))
              .setKey(convertExpr(Literal(ordinalValue.longValue() + 1, LongType)).getLiteral))
        }

      case e: GetMapValue if e.key.isInstanceOf[Literal] =>
        val value = e.key.asInstanceOf[Literal].value
        val dataType = e.key.asInstanceOf[Literal].dataType
        buildExprNode {
          _.setGetMapValueExpr(
            pb.PhysicalGetMapValueExprNode
              .newBuilder()
              .setExpr(convertExprWithFallback(e.child, isPruningExpr, fallback))
              .setKey(convertExpr(Literal(value, dataType)).getLiteral))
        }

      case e: GetStructField =>
        buildExprNode {
          _.setGetIndexedFieldExpr(
            pb.PhysicalGetIndexedFieldExprNode
              .newBuilder()
              .setExpr(convertExprWithFallback(e.child, isPruningExpr, fallback))
              .setKey(convertExpr(Literal(e.ordinal, IntegerType)).getLiteral))
        }

      case StubExpr("RowNum", _, _) =>
        buildExprNode {
          _.setRowNumExpr(pb.RowNumExprNode.newBuilder())
        }

      // hive UDFJson
      // hive UDFJson
      case e
          if udfJsonEnabled && (
            e.isInstanceOf[GetJsonObject]
              || getFunctionClassName(e).contains("org.apache.hadoop.hive.ql.udf.UDFJson")
          )
            && e.children.map(_.dataType) == Seq(StringType, StringType)
            && e.children(1).isInstanceOf[Literal] =>
        // use GetParsedJsonObject + ParseJson for reusing parsed json value in native
        val parsed = Shims.get.createNativeExprWrapper(
          buildExtScalarFunction("ParseJson", e.children(0) :: Nil, BinaryType),
          BinaryType,
          nullable = false)
        buildExtScalarFunction("GetParsedJsonObject", parsed :: e.children(1) :: Nil, StringType)

      // hive UDF brickhouse.array_union
      case e
          if getFunctionClassName(e).contains("brickhouse.udf.collect.ArrayUnionUDF")
            && udfBrickHouseEnabled =>
        buildExtScalarFunction("BrickhouseArrayUnion", e.children, e.dataType)

      case e =>
        Shims.get.convertMoreExprWithFallback(e, isPruningExpr, fallback) match {
          case Some(converted) => return converted
          case _ =>
        }
        fallback(e)
    }
  }

  def convertAggregateExpr(e: AggregateExpression): pb.PhysicalExprNode = {
    assert(Shims.get.getAggregateExpressionFilter(e).isEmpty)
    val aggBuilder = pb.PhysicalAggExprNode.newBuilder()
    aggBuilder.setReturnType(convertDataType(e.dataType))

    e.aggregateFunction match {
      case e: Max =>
        aggBuilder.setAggFunction(pb.AggFunction.MAX)
        aggBuilder.addChildren(convertExpr(e.child))
      case e: Min =>
        aggBuilder.setAggFunction(pb.AggFunction.MIN)
        aggBuilder.addChildren(convertExpr(e.child))
      case e: Sum if e.dataType.isInstanceOf[AtomicType] =>
        aggBuilder.setAggFunction(pb.AggFunction.SUM)
        aggBuilder.addChildren(convertExpr(e.child))
      case e: Average if e.dataType.isInstanceOf[AtomicType] =>
        aggBuilder.setAggFunction(pb.AggFunction.AVG)
        aggBuilder.addChildren(convertExpr(e.child))
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

      case First(child, ignoresNullExpr) =>
        val ignoresNull = ignoresNullExpr.asInstanceOf[Any] match {
          case Literal(v: Boolean, BooleanType) => v
          case v: Boolean => v
        }
        aggBuilder.setAggFunction(if (ignoresNull) {
          pb.AggFunction.FIRST_IGNORES_NULL
        } else {
          pb.AggFunction.FIRST
        })
        aggBuilder.addChildren(convertExpr(child))

      case CollectList(child, _, _) =>
        aggBuilder.setAggFunction(pb.AggFunction.COLLECT_LIST)
        aggBuilder.addChildren(convertExpr(child))
      case CollectSet(child, _, _) =>
        aggBuilder.setAggFunction(pb.AggFunction.COLLECT_SET)
        aggBuilder.addChildren(convertExpr(child))

      // brickhouse UDAFs
      case udaf
          if HiveUDFUtil
            .getFunctionClassName(udaf)
            .contains("brickhouse.udf.collect.CollectUDAF")
            && udfBrickHouseEnabled
            && udaf.children.size == 1
            && udaf.children.head.dataType.isInstanceOf[ArrayType] =>
        aggBuilder.setAggFunction(pb.AggFunction.BRICKHOUSE_COLLECT)
        aggBuilder.addChildren(convertExpr(udaf.children.head))
      case udaf
          if HiveUDFUtil
            .getFunctionClassName(udaf)
            .contains("brickhouse.udf.collect.CombineUniqueUDAF")
            && udfBrickHouseEnabled =>
        aggBuilder.setAggFunction(pb.AggFunction.BRICKHOUSE_COMBINE_UNIQUE)
        aggBuilder.addChildren(convertExpr(udaf.children.head))

      case udaf =>
        Shims.get.convertMoreAggregateExpr(e) match {
          case Some(converted) => return converted
          case _ =>
        }

        // fallback to UDAF
        if (AuronConf.UDAF_FALLBACK_ENABLE.booleanConf()) {
          udaf match {
            case _: DeclarativeAggregate =>
            case _: TypedImperativeAggregate[_] =>
            case u =>
              throw new NotImplementedError(
                s"Unsupported UDAF: ${u.getClass.getName}, expect" +
                  s" DeclarativeAggregate/TypeImperativeAggregate")
          }
          aggBuilder.setAggFunction(pb.AggFunction.UDAF)
          val convertedChildren = mutable.LinkedHashMap[pb.PhysicalExprNode, BoundReference]()
          val bound = udaf.mapChildren {
            case p: Literal => p
            case p =>
              val convertedChild = convertExpr(p)
              val nextBindIndex = convertedChildren.size
              convertedChildren.getOrElseUpdate(
                convertedChild,
                BoundReference(nextBindIndex, p.dataType, p.nullable))
          }

          val paramsSchema = StructType(
            convertedChildren.values
              .map(ref => StructField("", ref.dataType, ref.nullable))
              .toSeq)

          val serialized =
            serializeExpression(
              bound.asInstanceOf[AggregateFunction with Serializable],
              paramsSchema)

          aggBuilder.setUdaf(
            pb.AggUdaf
              .newBuilder()
              .setSerialized(ByteString.copyFrom(serialized))
              .setInputSchema(NativeConverters.convertSchema(paramsSchema)))
          aggBuilder.addAllChildren(convertedChildren.keys.asJava)
        } else {
          throw new NotImplementedError(
            s"unsupported aggregate expression: (${e.getClass})," +
              s" set ${AuronConf.UDAF_FALLBACK_ENABLE.key} true to enable UDAF fallbacking")
        }

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
      case _: ExistenceJoin => pb.JoinType.EXISTENCE
      case _ => throw new NotImplementedError(s"unsupported join type: ${joinType}")
    }
  }

  def buildBinaryExprNode(
      left: Expression,
      right: Expression,
      op: String,
      isPruningExpr: Boolean,
      fallback: Expression => pb.PhysicalExprNode): pb.PhysicalExprNode =
    buildExprNode {
      _.setBinaryExpr(
        pb.PhysicalBinaryExprNode
          .newBuilder()
          .setL(convertExprWithFallback(left, isPruningExpr, fallback))
          .setR(convertExprWithFallback(right, isPruningExpr, fallback))
          .setOp(op))
    }

  def buildScalarFunctionNode(
      fn: pb.ScalarFunction,
      args: Seq[Expression],
      dataType: DataType,
      isPruningExpr: Boolean,
      fallback: Expression => pb.PhysicalExprNode): pb.PhysicalExprNode =
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

  def buildExtScalarFunctionNode(
      name: String,
      args: Seq[Expression],
      dataType: DataType,
      isPruningExpr: Boolean,
      fallback: Expression => pb.PhysicalExprNode): pb.PhysicalExprNode =
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

  def unpackBinaryTypeCast(expr: Expression): Expression =
    expr match {
      case expr: Cast if expr.dataType == BinaryType => expr.child
      case expr => expr
    }

  def nullIfNegative(expr: Expression): Expression = {
    If(LessThanOrEqual(expr, Literal.default(expr.dataType)), Literal(null, expr.dataType), expr)
  }

  def serializeExpression[E <: Expression](
      expr: E with Serializable,
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

  def deserializeExpression[E <: Expression, S <: Serializable](
      serialized: Array[Byte]): (E with Serializable, S) = {
    Utils.tryWithResource(new ByteArrayInputStream(serialized)) { bis =>
      Utils.tryWithResource(new ObjectInputStream(bis)) { ois =>
        val expr = ois.readObject().asInstanceOf[E with Serializable]
        val payload = ois.readObject().asInstanceOf[S with Serializable]
        (expr, payload)
      }
    }
  }

  def prepareExecSubquery(subquery: ExecSubqueryExpression): Unit = {
    val isCanonicalized =
      MethodUtils.invokeMethod(subquery.plan, true, "isCanonicalizedPlan").asInstanceOf[Boolean]

    if (!isCanonicalized) {
      subquery match {
        case e if e.getClass.getName == "org.apache.spark.sql.execution.ScalarSubquery" =>
          if (!FieldUtils.readField(e, "updated", true).asInstanceOf[Boolean]) {
            e.updateResult()
          }
        case e if e.getClass.getName == "org.apache.spark.sql.execution.InSubquery" =>
          if (!FieldUtils.readField(e, "updated", true).asInstanceOf[Boolean]) {
            e.updateResult()
          }
        case e if e.getClass.getName == "org.apache.spark.sql.execution.InSubqueryExec" =>
          val result = FieldUtils.readField(e, "result", true)
          val resultBroadcast = FieldUtils.readField(e, "resultBroadcast", true)
          if (result == null && resultBroadcast == null) {
            e.updateResult()
          }
        case _ =>
      }
    }
  }

  case class StubExpr(
      name: String,
      override val dataType: DataType,
      override val nullable: Boolean)
      extends LeafExpression {
    override def eval(input: InternalRow): Any = null
    override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = null
  }
}
