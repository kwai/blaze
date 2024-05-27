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
import scala.collection.immutable.SortedMap

import org.apache.spark.OneToOneDependency
import org.apache.spark.internal.Logging
import org.apache.spark.sql.blaze.BlazeConf
import org.apache.spark.sql.blaze.MetricNode
import org.apache.spark.sql.blaze.NativeConverters
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
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.plans.physical.AllTuples
import org.apache.spark.sql.catalyst.plans.physical.ClusteredDistribution
import org.apache.spark.sql.catalyst.plans.physical.Distribution
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.plans.physical.UnspecifiedDistribution
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.DataType
import org.blaze.{protobuf => pb}
import org.apache.spark.sql.catalyst.expressions.ExprId
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.execution.aggregate.SortAggregateExec
import org.apache.spark.sql.execution.blaze.plan.NativeAggBase.AggExecMode
import org.apache.spark.sql.execution.blaze.plan.NativeAggBase.HashAgg
import org.apache.spark.sql.execution.blaze.plan.NativeAggBase.NativeAggrInfo
import org.apache.spark.sql.execution.blaze.plan.NativeAggBase.SortAgg
import org.apache.spark.sql.types.BinaryType

abstract class NativeAggBase(
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

  override lazy val metrics: Map[String, SQLMetric] = SortedMap[String, SQLMetric]() ++ Map(
    NativeHelper
      .getDefaultNativeMetrics(sparkContext)
      .filterKeys(Set(
        "stage_id",
        "output_rows",
        "elapsed_compute",
        "mem_spill_count",
        "mem_spill_size",
        "mem_spill_iotime",
        "disk_spill_size",
        "disk_spill_iotime",
        "input_batch_count",
        "input_batch_mem_size",
        "input_row_count"))
      .toSeq: _*)

  override def requiredChildDistribution: List[Distribution] = {
    requiredChildDistributionExpressions match {
      case Some(exprs) if exprs.isEmpty => AllTuples :: Nil
      case Some(exprs) => ClusteredDistribution(exprs) :: Nil
      case None => UnspecifiedDistribution :: Nil
    }
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = if (execMode == SortAgg) {
    groupingExpressions.map(SortOrder(_, Ascending)) :: Nil
  } else {
    Seq(Nil)
  }

  private def nativeAggrInfos: Seq[NativeAggrInfo] = aggregateExpressions
    .zip(aggregateAttributes)
    .map { case (aggr, aggrAttr) =>
      NativeAggBase.getNativeAggrInfo(aggr, aggrAttr)
    }

  private def nativeExecMode: pb.AggExecMode = execMode match {
    case HashAgg => pb.AggExecMode.HASH_AGG
    case SortAgg => pb.AggExecMode.SORT_AGG
  }

  private def nativeAggrs = nativeAggrInfos.flatMap(_.nativeAggrs)

  private def nativeGroupingExprs = groupingExpressions.map(NativeConverters.convertExpr(_))

  private def nativeGroupingNames = groupingExpressions.map(Util.getFieldNameByExprId)

  private def nativeAggrNames = nativeAggrInfos.map(_.outputAttr).map(_.name)

  private def nativeAggrModes = nativeAggrInfos.map(_.mode match {
    case Partial => pb.AggMode.PARTIAL
    case PartialMerge => pb.AggMode.PARTIAL_MERGE
    case Final => pb.AggMode.FINAL
    case Complete =>
      throw new NotImplementedError("aggrMode = Complete not yet supported")
  })

  // check whether native converting is supported
  nativeAggrs
  nativeGroupingExprs
  nativeGroupingNames
  nativeAggrs
  nativeAggrModes

  override def output: Seq[Attribute] =
    if (nativeAggrModes.contains(pb.AggMode.FINAL)) {
      groupingExpressions.map(_.toAttribute) ++ aggregateAttributes
    } else {
      groupingExpressions.map(_.toAttribute) :+
        AttributeReference(NativeAggBase.AGG_BUF_COLUMN_NAME, BinaryType, nullable = false)(
          ExprId.apply(NativeAggBase.AGG_BUF_COLUMN_EXPR_ID))
    }

  override def outputPartitioning: Partitioning =
    child.outputPartitioning

  private val supportsPartialSkipping = (
    BlazeConf.PARTIAL_AGG_SKIPPING_ENABLE.booleanConf()
      && (child match { // do not trigger skipping after ExpandExec
        case _: NativeExpandBase => false
        case c: NativeProjectBase if c.child.isInstanceOf[NativeExpandBase] => false
        case _ => true
      })
      && initialInputBufferOffset == 0
      && aggregateExpressions.forall(_.mode == Partial)
      && requiredChildDistribution.forall(_ == UnspecifiedDistribution)
  )

  override def doExecuteNative(): NativeRDD = {
    val inputRDD = NativeHelper.executeNative(child)
    val nativeMetrics = MetricNode(metrics, inputRDD.metrics :: Nil)
    val nativeExecMode = this.nativeExecMode
    val nativeAggrNames = this.nativeAggrNames
    val nativeGroupingNames = this.nativeGroupingNames
    val nativeAggrModes = this.nativeAggrModes
    val nativeAggrs = this.nativeAggrs
    val nativeGroupingExprs = this.nativeGroupingExprs

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
              .setSupportsPartialSkipping(supportsPartialSkipping)
              .setInput(inputPlan))
          .build()
      },
      friendlyName = s"NativeRDD.$execMode")
  }

  override val nodeName: String = execMode match {
    case HashAgg => "NativeHashAggregate"
    case SortAgg => "NativeSortAggregate"
  }
}

object NativeAggBase extends Logging {

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
        .mapChildren(e => createPlaceholder(e))
        .asInstanceOf[AggregateFunction],
      aggr.mode,
      aggr.isDistinct)
    val outputAttr =
      AttributeReference(Util.getFieldNameByExprId(aggrAttr), aggrAttr.dataType, aggr.nullable)(
        aggrAttr.exprId)

    aggr.mode match {
      case Partial =>
        NativeAggrInfo(aggr.mode, NativeConverters.convertAggregateExpr(aggr) :: Nil, outputAttr)

      case PartialMerge | Final =>
        NativeAggrInfo(
          aggr.mode,
          NativeConverters.convertAggregateExpr(reducedAggr) :: Nil,
          outputAttr)

      case Complete =>
        throw new NotImplementedError("aggrMode = Complete not yet supported")
    }
  }

  private def createPlaceholder(e: Expression): Expression = {
    e match {
      case _: Literal => e // no need to create placeholder
      case _ =>
        val placeholder = pb.PhysicalExprNode
          .newBuilder()
          .setScalarFunction(
            pb.PhysicalScalarFunctionNode
              .newBuilder()
              .setFun(pb.ScalarFunction.SparkExtFunctions)
              .setName("Placeholder")
              .setReturnType(NativeConverters.convertDataType(e.dataType)))
          .build()
        Shims.get.createNativeExprWrapper(placeholder, e.dataType, e.nullable)
    }
  }

  def findPreviousNativeAggrExec(exec: SparkPlan): Option[NativeAggBase] = {
    val isSortExec = exec.isInstanceOf[SortAggregateExec]

    @tailrec
    def findRecursive(exec: SparkPlan): Option[NativeAggBase] = {
      val passthroughNodeNames =
        Seq("QueryStage", "InputAdapter", "CustomShuffleRead", "AQEShuffleRead")
      exec match {
        case e: NativeAggBase => Some(e)
        case e: ReusedExchangeExec => findRecursive(e.child)
        case e: NativeShuffleExchangeBase => findRecursive(e.child)
        case e: NativeSortBase if isSortExec => findRecursive(e.child)
        case e: UnaryExecNode if passthroughNodeNames.exists(e.nodeName.contains(_)) =>
          findRecursive(e.child)
        case stageInput if Shims.get.isQueryStageInput(stageInput) =>
          findRecursive(Shims.get.getChildStage(stageInput))
        case _ => None
      }
    }

    // cannot find from a partial agg
    if (exec.requiredChildDistribution.isEmpty) {
      return None
    }
    findRecursive(exec.children.head)
  }
}
