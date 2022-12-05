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
import org.apache.spark.internal.Logging
import org.apache.spark.sql.blaze.MetricNode
import org.apache.spark.sql.blaze.NativeConverters
import org.apache.spark.sql.blaze.NativeConverters.NativeExprWrapper
import org.apache.spark.sql.blaze.NativeRDD
import org.apache.spark.sql.blaze.NativeHelper
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.physical.AllTuples
import org.apache.spark.sql.catalyst.plans.physical.ClusteredDistribution
import org.apache.spark.sql.catalyst.plans.physical.Distribution
import org.apache.spark.sql.catalyst.plans.physical.UnspecifiedDistribution
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.blaze.plan.NativeHashAggregateExec._
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.LongType
import org.apache.spark.OneToOneDependency
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.blaze.{protobuf => pb}
import org.apache.spark.sql.blaze.NativeSupports
import org.apache.spark.sql.blaze.Shims
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec

case class NativeHashAggregateExec(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    override val child: SparkPlan)
    extends UnaryExecNode
    with NativeSupports
    with Logging {

  override lazy val metrics: Map[String, SQLMetric] = Map(
    NativeHelper
      .getDefaultNativeMetrics(sparkContext)
      .filterKeys(Set("output_rows", "elapsed_compute"))
      .toSeq: _*)

  val aggrMode: AggregateMode =
    getAggrMode(aggregateExpressions, requiredChildDistributionExpressions)

  override def requiredChildDistribution: List[Distribution] = {
    requiredChildDistributionExpressions match {
      case Some(exprs) if exprs.isEmpty => AllTuples :: Nil
      case Some(exprs) => ClusteredDistribution(exprs) :: Nil
      case None => UnspecifiedDistribution :: Nil
    }
  }

  val nativeAggrMode: pb.AggregateMode =
    aggrMode match {
      case Partial | PartialMerge => pb.AggregateMode.PARTIAL
      case Final => pb.AggregateMode.FINAL
      case Complete =>
        throw new NotImplementedError("aggrMode = Complete not yet supported")
    }

  val previousNativeAggrExec: NativeHashAggregateExec = aggrMode match {
    case Partial => null
    case _ =>
      @tailrec
      def findPreviousNativeAggrExec(exec: SparkPlan = this.child): NativeHashAggregateExec = {
        findPreviousNativeAggrExec(exec match {
          case e: NativeHashAggregateExec => return e
          case unary: UnaryExecNode => unary.child
          case e: ReusedExchangeExec => e.child
          case stageInput if Shims.get.sparkPlanShims.isQueryStageInput(stageInput) =>
            Shims.get.sparkPlanShims.getChildStage(stageInput)
        })
      }
      findPreviousNativeAggrExec()
  }

  val nativeAggrInfos: Seq[NativeAggrInfo] = aggregateExpressions
    .zip(aggregateAttributes)
    .map {
      case (aggr, aggrAttr) => getNativeAggrInfo(aggr, aggrAttr)
    }

  val nativeOutputSchema: pb.Schema = {
    val schemaBuilder = Util.getNativeSchema(groupingExpressions).toBuilder
    for (aggrInfo <- nativeAggrInfos) {
      aggrMode match {
        case Partial | PartialMerge =>
          for ((attr, i) <- aggrInfo.outputPartialAttrs.zipWithIndex) {
            schemaBuilder.addColumns(
              pb.Field
                .newBuilder()
                .setName(attr.name)
                .setArrowType(aggrInfo.outputPartialNativeType(i))
                .setNullable(attr.nullable))
          }
        case Final =>
          for (attr <- aggrInfo.outputAttrs) {
            schemaBuilder.addColumns(
              pb.Field
                .newBuilder()
                .setName(Util.getFieldNameByExprId(attr))
                .setArrowType(NativeConverters.convertDataType(attr.dataType))
                .setNullable(attr.nullable))
          }
        case Complete =>
          throw new NotImplementedError("aggrMode = Complete not yet supported")
      }
    }
    schemaBuilder.build()
  }

  val nativeInputSchema: pb.Schema = {
    aggrMode match {
      case Partial => Util.getNativeSchema(child.output)
      case PartialMerge | Final => previousNativeAggrExec.nativeOutputSchema
      case Complete =>
        throw new NotImplementedError("aggrMode = Complete not yet supported")
    }
  }

  val nativeAggrs: Seq[pb.PhysicalExprNode] =
    nativeAggrInfos.flatMap(_.nativeAggrs)

  val nativeGroupingExprs: Seq[pb.PhysicalExprNode] =
    groupingExpressions.map(NativeConverters.convertExpr(_))

  val nativeGroupingNames: Seq[String] =
    groupingExpressions.map(Util.getFieldNameByExprId)

  val nativeAggrNames: Seq[String] =
    nativeAggrInfos.flatMap(_.outputAttrs).map(_.name)

  override def output: Seq[Attribute] =
    (groupingExpressions ++ nativeAggrInfos.flatMap(_.outputPartialAttrs)).map(_.toAttribute)

  override def outputPartitioning: Partitioning =
    child.outputPartitioning

  override def doExecuteNative(): NativeRDD = {
    val inputRDD = NativeHelper.executeNative(child)
    val nativeMetrics = MetricNode(metrics, inputRDD.metrics :: Nil)

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      rddPartitions = inputRDD.partitions,
      rddDependencies = new OneToOneDependency(inputRDD) :: Nil,
      inputRDD.shuffleReadFull,
      (partition, taskContext) => {

        lazy val inputPlan =
          inputRDD.nativePlan(inputRDD.partitions(partition.index), taskContext)

        pb.PhysicalPlanNode
          .newBuilder()
          .setHashAggregate(
            pb.HashAggregateExecNode
              .newBuilder()
              .setMode(nativeAggrMode)
              .addAllAggrExprName(nativeAggrNames.asJava)
              .addAllGroupExprName(nativeGroupingNames.asJava)
              .addAllAggrExpr(nativeAggrs.asJava)
              .addAllGroupExpr(nativeGroupingExprs.asJava)
              .setInput(inputPlan)
              .setInputSchema(nativeInputSchema))
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

  override def withNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
    copy(child = newChildren.head)

  override val nodeName: String =
    s"NativeHashAggregate.$aggrMode"
}
object NativeHashAggregateExec {
  def getAggrMode(
      aggregateExpressions: Seq[AggregateExpression],
      requiredChildDistributionExpressions: Option[Seq[Expression]]): AggregateMode = {
    if (aggregateExpressions.exists(_.mode == Final)
      || (aggregateExpressions.isEmpty && requiredChildDistributionExpressions.isDefined)) {
      Final
    } else if (aggregateExpressions.exists(_.mode == PartialMerge)) {
      PartialMerge
    } else {
      Partial
    }
  }

  case class NativeAggrPartialState(
      stateAttr: Attribute,
      partialMerger: pb.PhysicalExprNode,
      arrowType: pb.ArrowType)

  object NativeAggrPartialState {
    def apply(
        aggrAttr: Attribute,
        stateFieldName: String,
        dataType: DataType,
        nullable: Boolean,
        partialMerger: AggregateFunction,
        arrowType: pb.ArrowType = null): NativeAggrPartialState = {

      val fieldName = s"${Util.getFieldNameByExprId(aggrAttr)}[$stateFieldName]"
      val stateAttr = AttributeReference(fieldName, dataType, nullable)(aggrAttr.exprId)
      val nativePartialMerger = NativeConverters.convertExpr(partialMerger)
      NativeAggrPartialState(
        stateAttr,
        nativePartialMerger,
        arrowType = Option(arrowType).getOrElse(NativeConverters.convertDataType(dataType)))
    }
  }

  def getDFRowHashPartialStates(
      aggr: AggregateExpression,
      aggrAttr: Attribute): Seq[NativeAggrPartialState] = {

    val uint64Type =
      pb.ArrowType.newBuilder().setUINT64(pb.EmptyMessage.getDefaultInstance).build()
    val aggrDataType = aggr.dataType
    val aggrNullable = aggr.nullable
    val buildPartialState = (
        fieldName: String,
        dataType: DataType,
        nullable: Boolean,
        partialMerger: AggregateFunction,
        nativeDataType: pb.ArrowType) => {
      NativeAggrPartialState(
        aggrAttr,
        fieldName,
        dataType,
        nullable,
        partialMerger,
        nativeDataType)
    }
    val buildColumn = (fieldName: String) => {
      NativeExprWrapper(
        pb.PhysicalExprNode
          .newBuilder()
          .setColumn(pb.PhysicalColumn
            .newBuilder()
            .setName(s"${Util.getFieldNameByExprId(aggrAttr)}[$fieldName]"))
          .build())
    }

    aggr.aggregateFunction match {
      case _: Sum =>
        // [sum]: input.data_type
        val partialMerger = Sum(buildColumn("sum"))
        Seq(buildPartialState("sum", aggrDataType, aggrNullable, partialMerger, null))

      case _: Average =>
        // [count]: u64
        // [sum]: input.data_type
        val partialMerger1 = Sum(buildColumn("count"))
        val partialMerger2 = Sum(buildColumn("sum"))
        Seq(
          buildPartialState("count", LongType, true, partialMerger1, uint64Type),
          buildPartialState("sum", aggrDataType, aggrNullable, partialMerger2, null))

      case _: Count =>
        // [count]: i64
        val partialMerger = Sum(buildColumn("count"))
        Seq(buildPartialState("count", LongType, true, partialMerger, null))

      case _: Max =>
        // [max]: input.data_type
        val partialMerger = Max(buildColumn("max"))
        Seq(buildPartialState("max", aggrDataType, aggrNullable, partialMerger, null))

      case _: Min =>
        // [min]: input.data_type
        val partialMerger = Max(buildColumn("min"))
        Seq(buildPartialState("min", aggrDataType, aggrNullable, partialMerger, null))

      case _ =>
        throw new NotImplementedError(
          s"aggregate function not supported: ${aggr.aggregateFunction}")
    }
  }

  case class NativeAggrInfo(
      nativeAggrs: Seq[pb.PhysicalExprNode],
      outputPartialAttrs: Seq[Attribute],
      outputPartialNativeType: Seq[pb.ArrowType],
      outputAttrs: Seq[Attribute])

  def getNativeAggrInfo(aggr: AggregateExpression, aggrAttr: Attribute): NativeAggrInfo = {
    aggr.mode match {
      case Partial =>
        val partialStates = getDFRowHashPartialStates(aggr, aggrAttr)
        NativeAggrInfo(
          nativeAggrs = NativeConverters.convertExpr(aggr) :: Nil,
          outputPartialAttrs = partialStates.map(_.stateAttr),
          outputPartialNativeType = partialStates.map(_.arrowType),
          outputAttrs = Seq(
            AttributeReference(
              Util.getFieldNameByExprId(aggrAttr),
              aggrAttr.dataType,
              aggr.nullable)(aggrAttr.exprId)))

      case PartialMerge =>
        val partialStates = getDFRowHashPartialStates(aggr, aggrAttr)
        NativeAggrInfo(
          nativeAggrs = partialStates.map(_.partialMerger),
          outputPartialAttrs = partialStates.map(_.stateAttr),
          outputPartialNativeType = partialStates.map(_.arrowType),
          outputAttrs = partialStates.map(_.stateAttr))

      case Final =>
        val reducedAggr = AggregateExpression(
          aggr.aggregateFunction
            .mapChildren(e => createPlaceholder(NativeConverters.convertDataType(e.dataType)))
            .asInstanceOf[AggregateFunction],
          aggr.mode,
          aggr.isDistinct)
        NativeAggrInfo(
          nativeAggrs = NativeConverters.convertExpr(reducedAggr) :: Nil,
          outputPartialAttrs = aggrAttr :: Nil,
          outputPartialNativeType = NativeConverters.convertDataType(aggr.dataType) :: Nil,
          outputAttrs = Seq(
            AttributeReference(
              Util.getFieldNameByExprId(aggrAttr),
              aggrAttr.dataType,
              aggrAttr.nullable)(aggrAttr.exprId)))

      case Complete =>
        throw new NotImplementedError("aggrMode = Complete not yet supported")
    }
  }

  private def createPlaceholder(nativeDataType: pb.ArrowType): Expression = {
    NativeExprWrapper(
      pb.PhysicalExprNode
        .newBuilder()
        .setScalarFunction(
          pb.PhysicalScalarFunctionNode
            .newBuilder()
            .setFun(pb.ScalarFunction.SparkExtFunctions)
            .setName("Placeholder")
            .setReturnType(nativeDataType))
        .build())
  }
}
