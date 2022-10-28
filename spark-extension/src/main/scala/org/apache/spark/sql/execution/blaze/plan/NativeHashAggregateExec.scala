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

package org.apache.spark.sql.execution.blaze.plan

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.blaze.MetricNode
import org.apache.spark.sql.blaze.NativeConverters
import org.apache.spark.sql.blaze.NativeRDD
import org.apache.spark.sql.blaze.NativeSupports
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.aggregate
import org.apache.spark.sql.catalyst.expressions.aggregate.Average
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.expressions.aggregate.Final
import org.apache.spark.sql.catalyst.expressions.aggregate.Max
import org.apache.spark.sql.catalyst.expressions.aggregate.Min
import org.apache.spark.sql.catalyst.expressions.aggregate.Partial
import org.apache.spark.sql.catalyst.expressions.aggregate.PartialMerge
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.catalyst.plans.physical.AllTuples
import org.apache.spark.sql.catalyst.plans.physical.ClusteredDistribution
import org.apache.spark.sql.catalyst.plans.physical.Distribution
import org.apache.spark.sql.catalyst.plans.physical.UnspecifiedDistribution
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.adaptive.ShuffleQueryStageInput
import org.apache.spark.sql.execution.adaptive.SkewedShuffleQueryStageInput
import org.apache.spark.sql.execution.blaze.plan.NativeHashAggregateExec.getDFRowHashStateFields
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.blaze.protobuf.AggregateFunction
import org.blaze.protobuf.AggregateMode
import org.blaze.protobuf.ArrowType
import org.blaze.protobuf.EmptyMessage
import org.blaze.protobuf.Field
import org.blaze.protobuf.HashAggregateExecNode
import org.blaze.protobuf.PhysicalAggregateExprNode
import org.blaze.protobuf.PhysicalExprNode
import org.blaze.protobuf.PhysicalPlanNode
import org.blaze.protobuf.Schema

case class NativeHashAggregateExec(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    override val child: SparkPlan)
    extends UnaryExecNode
    with NativeSupports
    with Logging {

  assert(
    !aggregateExpressions.exists(_.isDistinct),
    "native distinct aggregation is not yet supported")

  override lazy val metrics: Map[String, SQLMetric] =
    NativeSupports.getDefaultNativeMetrics(sparkContext)

  val aggrMode: aggregate.AggregateMode = if (requiredChildDistributionExpressions.isEmpty) {
    Partial
  } else if (aggregateExpressions.exists(_.mode == PartialMerge)) {
    PartialMerge
  } else {
    Final
  }

  override def requiredChildDistribution: List[Distribution] = {
    requiredChildDistributionExpressions match {
      case Some(exprs) if exprs.isEmpty => AllTuples :: Nil
      case Some(exprs) => ClusteredDistribution(exprs) :: Nil
      case None => UnspecifiedDistribution :: Nil
    }
  }

  override def output: Seq[Attribute] = {
    val columns = ArrayBuffer[NamedExpression]()
    columns ++= groupingExpressions
    columns ++= (aggrMode match {
      case Partial =>
        aggregateAttributes.zip(aggregateExpressions).flatMap {
          case (aggrAttr, aggr) => getDFRowHashStateFields(aggr, aggrAttr).map(_._1)
        }
      case PartialMerge | Final =>
        aggregateAttributes
    })
    columns.map(_.toAttribute)
  }

  val relatedPartialAggr: NativeHashAggregateExec = aggrMode match {
    case Partial => null
    case PartialMerge | Final =>
      @tailrec
      def findRelatedPartialAggr(exec: SparkPlan = this): NativeHashAggregateExec =
        exec match {
          case e: NativeHashAggregateExec if e.aggrMode == Partial => e
          case e =>
            findRelatedPartialAggr(e match {
              case stageInput: ShuffleQueryStageInput => stageInput.childStage
              case stageInput: SkewedShuffleQueryStageInput => stageInput.childStage
              case e: UnaryExecNode => e.child
              case e =>
                throw new NotImplementedError(
                  s"expect partial NativeHashAggregateExec, got ${e.nodeName}")
            })
        }

      findRelatedPartialAggr()
  }

  val nativePartialOutputSchema: Schema = aggrMode match {
    case Partial =>
      Schema
        .newBuilder()
        .addAllColumns(
          groupingExpressions
            .map(a =>
              NativeConverters.convertField(
                StructField(s"#${a.exprId.id}", a.dataType, a.nullable, a.metadata)))
            .asJava)
        .addAllColumns(aggregateAttributes
          .zip(aggregateExpressions)
          .flatMap {
            case (aggrAttr, aggr) =>
              getDFRowHashStateFields(aggr, aggrAttr).map {
                case (a, nativeType) =>
                  Field
                    .newBuilder()
                    .setName(a.name)
                    .setArrowType(nativeType)
                    .setNullable(a.nullable)
                    .build()
              }
          }
          .asJava)
        .build()
    case PartialMerge | Final =>
      relatedPartialAggr.nativePartialOutputSchema
  }

  val nativeInputSchema: Schema = aggrMode match {
    case Partial =>
      NativeConverters.convertSchema(StructType(child.output.map(a =>
        StructField(s"#${a.exprId.id}", a.dataType, a.nullable, a.metadata))))
    case PartialMerge | Final =>
      relatedPartialAggr.nativeInputSchema
  }

  val nativeGroupingExprs: Seq[PhysicalExprNode] = aggrMode match {
    case Partial =>
      groupingExpressions.map(expr => NativeConverters.convertExpr(expr))
    case PartialMerge | Final =>
      relatedPartialAggr.nativeGroupingExprs
  }

  val nativeAggrExprs: Seq[PhysicalExprNode] = aggrMode match {
    case Partial =>
      aggregateExpressions.map(aggr => NativeConverters.convertExpr(aggr.aggregateFunction))
    case PartialMerge | Final =>
      relatedPartialAggr.nativeAggrExprs
  }

  logWarning(s"XXX mode=$aggrMode, nativePartialOutputSchema: $nativePartialOutputSchema")
  logWarning(s"XXX mode=$aggrMode, nativeInputSchema: $nativeInputSchema")
  logWarning(s"XXX mode=$aggrMode, nativeGroupingExprs: $nativeGroupingExprs")
  logWarning(s"XXX mode=$aggrMode, nativeAggrExprs: $nativeAggrExprs")

  override def doExecuteNative(): NativeRDD = {
    val inputRDD = NativeSupports.executeNative(child)
    val nativeMetrics = MetricNode(metrics, inputRDD.metrics :: Nil)

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      inputRDD.partitions,
      inputRDD.dependencies,
      inputRDD.shuffleReadFull,
      (partition, taskContext) => {

        lazy val inputPlan =
          inputRDD.nativePlan(inputRDD.partitions(partition.index), taskContext)

        val (groupNames, aggrNames) = (
          groupingExpressions.map(a => s"#${a.exprId.id}"),
          aggregateAttributes.map(a => s"#${a.exprId.id}"))

        PhysicalPlanNode
          .newBuilder()
          .setHashAggregate(
            HashAggregateExecNode
              .newBuilder()
              .addAllAggrExprName(aggrNames.asJava)
              .addAllGroupExprName(groupNames.asJava)
              .addAllAggrExpr(nativeAggrExprs.asJava)
              .addAllGroupExpr(nativeGroupingExprs.asJava)
              .setInput(inputPlan)
              .setInputSchema(nativeInputSchema)
              .setMode(aggrMode match {
                case Partial => AggregateMode.PARTIAL
                case PartialMerge | Final => AggregateMode.FINAL
              }))
          .build()
      },
      friendlyName = "NativeRDD.HashAggregate")
  }

  override def doCanonicalize(): SparkPlan =
    HashAggregateExec(
      requiredChildDistributionExpressions,
      groupingExpressions,
      aggregateExpressions,
      aggregateAttributes,
      initialInputBufferOffset = 0,
      resultExpressions = Nil,
      child).canonicalized

  override val nodeName: String =
    s"NativeHashAggregate.$aggrMode"
}
object NativeHashAggregateExec {
  def getDFRowHashStateFields(
      aggr: AggregateExpression,
      aggrAttr: Attribute): Seq[(Attribute, ArrowType)] = {

    def makeFieldName(fieldName: String) = s"#${aggrAttr.exprId.id}[$fieldName]"

    def makeField(
        fieldName: String,
        dataType: DataType,
        nullable: Boolean,
        nativeType: ArrowType = null) = {
      (
        AttributeReference(makeFieldName(fieldName), dataType, nullable)(aggrAttr.exprId),
        Option(nativeType).getOrElse(NativeConverters.convertDataType(dataType)))
    }

    val uint64Type = ArrowType.newBuilder().setUINT64(EmptyMessage.getDefaultInstance).build()
    val aggrDataType = aggr.dataType
    val aggrNullable = aggr.nullable

    aggr.aggregateFunction match {
      case _: Sum => Seq(makeField("sum", aggrDataType, aggrNullable))
      case _: Average =>
        Seq(
          makeField("count", LongType, aggrNullable, uint64Type),
          makeField("sum", aggrDataType, aggrNullable))

      case _: Count => Seq(makeField("count", LongType, nullable = true, uint64Type))
      case _: Max => Seq(makeField("max", aggrDataType, nullable = true))
      case _: Min => Seq(makeField("min", aggrDataType, nullable = true))

      case _ =>
        throw new NotImplementedError(
          s"aggregate function not supported: ${aggr.aggregateFunction}")
    }
  }

  def getDFPartialMergeAggExprs(
      aggr: AggregateExpression,
      aggrAttr: Attribute): Seq[(Attribute, PhysicalExprNode, ArrowType)] = {

    def convertAggrExprWithNewFunction(newFunction: AggregateFunction): PhysicalExprNode = {
      val converted = NativeConverters.convertExpr(aggr)
      val withNewFunction = converted.getAggregateExpr.toBuilder
        .setAggrFunction(newFunction)
      PhysicalExprNode.newBuilder().setAggregateExpr(withNewFunction).build()
    }

    (aggr.aggregateFunction match {
      case _: Sum =>
        getDFRowHashStateFields(aggr, aggrAttr).zip(
          Seq(convertAggrExprWithNewFunction(AggregateFunction.SUM)))
      case _: Average =>
        getDFRowHashStateFields(aggr, aggrAttr).zip(
          Seq(
            convertAggrExprWithNewFunction(AggregateFunction.COUNT),
            convertAggrExprWithNewFunction(AggregateFunction.SUM)))
      case _: Count =>
        getDFRowHashStateFields(aggr, aggrAttr).zip(
          Seq(convertAggrExprWithNewFunction(AggregateFunction.COUNT)))
      case _: Max =>
        getDFRowHashStateFields(aggr, aggrAttr).zip(
          Seq(convertAggrExprWithNewFunction(AggregateFunction.MAX)))
      case _: Min =>
        getDFRowHashStateFields(aggr, aggrAttr).zip(
          Seq(convertAggrExprWithNewFunction(AggregateFunction.MIN)))
      case _ =>
        throw new NotImplementedError(
          s"aggregate function not supported: ${aggr.aggregateFunction}")
    }).map {
      case ((attr, arrowType), partialAggr) => (attr, partialAggr, arrowType)
    }
  }
}
