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
import scala.collection.mutable

import org.apache.spark.OneToOneDependency

import org.apache.spark.internal.Logging
import org.apache.spark.sql.blaze.MetricNode
import org.apache.spark.sql.blaze.NativeConverters
import org.apache.spark.sql.blaze.NativeConverters.NativeExprWrapper
import org.apache.spark.sql.blaze.NativeHelper
import org.apache.spark.sql.blaze.NativeRDD
import org.apache.spark.sql.blaze.NativeSupports
import org.apache.spark.sql.blaze.Shims
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.physical.AllTuples
import org.apache.spark.sql.catalyst.plans.physical.ClusteredDistribution
import org.apache.spark.sql.catalyst.plans.physical.Distribution
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.plans.physical.UnspecifiedDistribution
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.DataType
import org.blaze.{protobuf => pb}

import org.apache.spark.sql.catalyst.expressions.ExprId
import org.apache.spark.sql.execution.blaze.plan.NativeAggExec._
import org.apache.spark.sql.types.BinaryType

case class NativeAggExec(
    execMode: AggExecMode,
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    override val child: SparkPlan)
    extends UnaryExecNode
    with NativeSupports
    with Logging {

  override lazy val metrics: Map[String, SQLMetric] = mutable
    .LinkedHashMap(
      NativeHelper
        .getDefaultNativeMetrics(sparkContext)
        .filterKeys(Set("output_rows", "elapsed_compute", "spilled_bytes"))
        .toSeq: _*)
    .toMap

  override def requiredChildDistribution: List[Distribution] = {
    requiredChildDistributionExpressions match {
      case Some(exprs) if exprs.isEmpty => AllTuples :: Nil
      case Some(exprs) => ClusteredDistribution(exprs) :: Nil
      case None => UnspecifiedDistribution :: Nil
    }
  }

  private val _ensurePreviousNativeAggrExecExists: Unit = {
    @tailrec
    def findPreviousNativeAggrExec(exec: SparkPlan = this.child): NativeAggExec = {
      findPreviousNativeAggrExec(exec match {
        case e: NativeAggExec => return e
        case e: ReusedExchangeExec => e.child
        case e: NativeShuffleExchangeBase => e.child
        case e: NativeSortExec if execMode == SortAgg => e.child
        case e: UnaryExecNode if e.nodeName.contains("QueryStage") => e.child
        case e: UnaryExecNode if e.nodeName.contains("InputAdapter") => e.child
        case stageInput if Shims.get.sparkPlanShims.isQueryStageInput(stageInput) =>
          Shims.get.sparkPlanShims.getChildStage(stageInput)
        case e =>
          throw new RuntimeException(
            s"cannot find previous native aggregate, matching: ${e.getClass}")
      })
    }
    if (requiredChildDistributionExpressions.isDefined) {
      assert(findPreviousNativeAggrExec() != null)
    }
  }

  val nativeAggrInfos: Seq[NativeAggrInfo] = aggregateExpressions
    .zip(aggregateAttributes)
    .map {
      case (aggr, aggrAttr) => getNativeAggrInfo(aggr, aggrAttr)
    }

  val nativeExecMode: pb.AggExecMode = execMode match {
    case HashAgg => pb.AggExecMode.HASH_AGG
    case SortAgg => pb.AggExecMode.SORT_AGG
  }

  val nativeAggrs: Seq[pb.PhysicalExprNode] =
    nativeAggrInfos.flatMap(_.nativeAggrs)

  val nativeGroupingExprs: Seq[pb.PhysicalExprNode] =
    groupingExpressions.map(NativeConverters.convertExpr(_))

  val nativeGroupingNames: Seq[String] =
    groupingExpressions.map(Util.getFieldNameByExprId)

  val nativeAggrNames: Seq[String] =
    nativeAggrInfos.map(_.outputAttr).map(_.name)

  val nativeAggrModes: Seq[pb.AggMode] =
    nativeAggrInfos.map(_.mode match {
      case Partial => pb.AggMode.PARTIAL
      case PartialMerge => pb.AggMode.PARTIAL_MERGE
      case Final => pb.AggMode.FINAL
      case Complete =>
        throw new NotImplementedError("aggrMode = Complete not yet supported")
    })
  override def output: Seq[Attribute] =
    if (nativeAggrModes.contains(pb.AggMode.FINAL)) {
      groupingExpressions.map(_.toAttribute) ++ aggregateAttributes
    } else {
      groupingExpressions.map(_.toAttribute) :+
        AttributeReference(AGG_BUF_COLUMN_NAME, BinaryType, nullable = false)(
          ExprId.apply(AGG_BUF_COLUMN_EXPR_ID))
    }

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
      inputRDD.isShuffleReadFull,
      (partition, taskContext) => {

        lazy val inputPlan =
          inputRDD.nativePlan(inputRDD.partitions(partition.index), taskContext)

        pb.PhysicalPlanNode
          .newBuilder()
          .setAgg(
            pb.AggExecNode
              .newBuilder()
              .setExecMode(nativeExecMode)
              .addAllAggExprName(nativeAggrNames.asJava)
              .addAllGroupingExprName(nativeGroupingNames.asJava)
              .addAllMode(nativeAggrModes.asJava)
              .addAllAggExpr(nativeAggrs.asJava)
              .addAllGroupingExpr(nativeGroupingExprs.asJava)
              .setInitialInputBufferOffset(initialInputBufferOffset)
              .setInput(inputPlan))
          .build()
      },
      friendlyName = s"NativeRDD.$execMode")
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

  override val nodeName: String = execMode match {
    case HashAgg => "NativeHashAggregate"
    case SortAgg => "NativeSortAggregate"
  }
}

object NativeAggExec {
  val AGG_BUF_COLUMN_EXPR_ID = 9223372036854775807L
  val AGG_BUF_COLUMN_NAME = s"#$AGG_BUF_COLUMN_EXPR_ID"

  trait AggExecMode;
  case object HashAgg extends AggExecMode
  case object SortAgg extends AggExecMode

  case class NativeAggrPartialState(stateAttr: Attribute, arrowType: pb.ArrowType)

  object NativeAggrPartialState {
    def apply(
        aggrAttr: Attribute,
        stateFieldName: String,
        dataType: DataType,
        nullable: Boolean,
        arrowType: pb.ArrowType = null): NativeAggrPartialState = {

      val fieldName = s"${Util.getFieldNameByExprId(aggrAttr)}[$stateFieldName]"
      val stateAttr = AttributeReference(fieldName, dataType, nullable)(aggrAttr.exprId)
      NativeAggrPartialState(
        stateAttr,
        arrowType = Option(arrowType).getOrElse(NativeConverters.convertDataType(dataType)))
    }
  }

  case class NativeAggrInfo(
      mode: AggregateMode,
      nativeAggrs: Seq[pb.PhysicalExprNode],
      outputAttr: Attribute)

  def getNativeAggrInfo(aggr: AggregateExpression, aggrAttr: Attribute): NativeAggrInfo = {
    val reducedAggr = AggregateExpression(
      aggr.aggregateFunction
        .mapChildren(e => createPlaceholder(NativeConverters.convertDataType(e.dataType)))
        .asInstanceOf[AggregateFunction],
      aggr.mode,
      aggr.isDistinct)
    val outputAttr =
      AttributeReference(Util.getFieldNameByExprId(aggrAttr), aggrAttr.dataType, aggr.nullable)(
        aggrAttr.exprId)

    aggr.mode match {
      case Partial =>
        NativeAggrInfo(aggr.mode, NativeConverters.convertExpr(aggr) :: Nil, outputAttr)

      case PartialMerge | Final =>
        NativeAggrInfo(aggr.mode, NativeConverters.convertExpr(reducedAggr) :: Nil, outputAttr)

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
