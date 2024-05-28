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

import scala.collection.JavaConverters._
import scala.collection.immutable.SortedMap

import org.apache.spark.OneToOneDependency
import org.apache.spark.sql.blaze.MetricNode
import org.apache.spark.sql.blaze.NativeConverters
import org.apache.spark.sql.blaze.NativeHelper
import org.apache.spark.sql.blaze.NativeRDD
import org.apache.spark.sql.blaze.NativeSupports
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.NullsFirst
import org.apache.spark.sql.catalyst.expressions.Rank
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.physical.AllTuples
import org.apache.spark.sql.catalyst.plans.physical.ClusteredDistribution
import org.apache.spark.sql.catalyst.plans.physical.Distribution
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.metric.SQLMetric
import org.blaze.{protobuf => pb}
import org.apache.spark.sql.catalyst.expressions.DenseRank
import org.apache.spark.sql.catalyst.expressions.RowNumber
import org.apache.spark.sql.catalyst.expressions.WindowExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.Average
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.expressions.aggregate.Max
import org.apache.spark.sql.catalyst.expressions.aggregate.Min
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum

abstract class NativeWindowBase(
    windowExpression: Seq[NamedExpression],
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    override val child: SparkPlan)
    extends UnaryExecNode
    with NativeSupports {

  override lazy val metrics: Map[String, SQLMetric] = SortedMap[String, SQLMetric]() ++ Map(
    NativeHelper
      .getDefaultNativeMetrics(sparkContext)
      .filterKeys(Set("stage_id", "output_rows", "elapsed_compute"))
      .toSeq: _*)

  override def output: Seq[Attribute] = child.output ++ windowExpression.map(_.toAttribute)
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def requiredChildDistribution: Seq[Distribution] = {
    if (partitionSpec.isEmpty) {
      AllTuples :: Nil
    } else {
      ClusteredDistribution(partitionSpec) :: Nil
    }
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(partitionSpec.map(SortOrder(_, Ascending)) ++ orderSpec)

  private def nativeWindowExprs = windowExpression.map { named =>
    val field = NativeConverters.convertField(Util.getSchema(named :: Nil).fields(0))
    val windowExprBuilder = pb.WindowExprNode.newBuilder().setField(field)

    named.children.head match {
      case WindowExpression(function, spec) =>
        function match {
          case e @ RowNumber() =>
            assert(
              spec.frameSpecification == e.frame,
              s"window frame not supported: ${spec.frameSpecification}")
            windowExprBuilder.setFuncType(pb.WindowFunctionType.Window)
            windowExprBuilder.setWindowFunc(pb.WindowFunction.ROW_NUMBER)

          case e: Rank =>
            assert(
              spec.frameSpecification == e.frame,
              s"window frame not supported: ${spec.frameSpecification}")
            windowExprBuilder.setFuncType(pb.WindowFunctionType.Window)
            windowExprBuilder.setWindowFunc(pb.WindowFunction.RANK)

          case e: DenseRank =>
            assert(
              spec.frameSpecification == e.frame,
              s"window frame not supported: ${spec.frameSpecification}")
            windowExprBuilder.setFuncType(pb.WindowFunctionType.Window)
            windowExprBuilder.setWindowFunc(pb.WindowFunction.DENSE_RANK)

          case e: Sum =>
            assert(
              spec.frameSpecification == RowNumber().frame, // only supports RowFrame(Unbounde, CurrentRow)
              s"window frame not supported: ${spec.frameSpecification}")
            windowExprBuilder.setFuncType(pb.WindowFunctionType.Agg)
            windowExprBuilder.setAggFunc(pb.AggFunction.SUM)
            windowExprBuilder.addChildren(NativeConverters.convertExpr(e.child))

          case e: Average =>
            assert(
              spec.frameSpecification == RowNumber().frame, // only supports RowFrame(Unbounded, CurrentRow)
              s"window frame not supported: ${spec.frameSpecification}")
            windowExprBuilder.setFuncType(pb.WindowFunctionType.Agg)
            windowExprBuilder.setAggFunc(pb.AggFunction.AVG)
            windowExprBuilder.addChildren(NativeConverters.convertExpr(e.child))

          case e: Max =>
            assert(
              spec.frameSpecification == RowNumber().frame, // only supports RowFrame(Unbounded, CurrentRow)
              s"window frame not supported: ${spec.frameSpecification}")
            windowExprBuilder.setFuncType(pb.WindowFunctionType.Agg)
            windowExprBuilder.setAggFunc(pb.AggFunction.MAX)
            windowExprBuilder.addChildren(NativeConverters.convertExpr(e.child))

          case e: Min =>
            assert(
              spec.frameSpecification == RowNumber().frame, // only supports RowFrame(Unbounded, CurrentRow)
              s"window frame not supported: ${spec.frameSpecification}")
            windowExprBuilder.setFuncType(pb.WindowFunctionType.Agg)
            windowExprBuilder.setAggFunc(pb.AggFunction.MIN)
            windowExprBuilder.addChildren(NativeConverters.convertExpr(e.child))

          case Count(child :: Nil) =>
            assert(
              spec.frameSpecification == RowNumber().frame, // only supports RowFrame(Unbounded, CurrentRow)
              s"window frame not supported: ${spec.frameSpecification}")
            windowExprBuilder.setFuncType(pb.WindowFunctionType.Agg)
            windowExprBuilder.setAggFunc(pb.AggFunction.COUNT)
            windowExprBuilder.addChildren(NativeConverters.convertExpr(child))

          case other =>
            throw new NotImplementedError(s"window function not supported: $other")
        }
      case other =>
        throw new NotImplementedError(s"expect WindowExpression, got: $other")
    }
    windowExprBuilder.build()
  }

  private def nativePartitionSpecExprs = partitionSpec.map { partition =>
    NativeConverters.convertExpr(partition)
  }

  private def nativeOrderSpecExprs = orderSpec.map { sortOrder =>
    pb.PhysicalExprNode
      .newBuilder()
      .setSort(
        pb.PhysicalSortExprNode
          .newBuilder()
          .setExpr(NativeConverters.convertExpr(sortOrder.child))
          .setAsc(sortOrder.direction == Ascending)
          .setNullsFirst(sortOrder.nullOrdering == NullsFirst)
          .build())
      .build()
  }

  // check whether native converting is supported
  nativeWindowExprs
  nativeOrderSpecExprs
  nativePartitionSpecExprs

  override def doExecuteNative(): NativeRDD = {
    val inputRDD = NativeHelper.executeNative(child)
    val nativeMetrics = MetricNode(metrics, inputRDD.metrics :: Nil)
    val nativeWindowExprs = this.nativeWindowExprs
    val nativeOrderSpecExprs = this.nativeOrderSpecExprs
    val nativePartitionSpecExprs = this.nativePartitionSpecExprs

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      rddPartitions = inputRDD.partitions,
      rddDependencies = new OneToOneDependency(inputRDD) :: Nil,
      inputRDD.isShuffleReadFull,
      (partition, taskContext) => {
        val inputPartition = inputRDD.partitions(partition.index)
        val nativeWindowExec = pb.WindowExecNode
          .newBuilder()
          .setInput(inputRDD.nativePlan(inputPartition, taskContext))
          .addAllWindowExpr(nativeWindowExprs.asJava)
          .addAllPartitionSpec(nativePartitionSpecExprs.asJava)
          .addAllOrderSpec(nativeOrderSpecExprs.asJava)
          .build()
        pb.PhysicalPlanNode.newBuilder().setWindow(nativeWindowExec).build()
      },
      friendlyName = "NativeRDD.Window")
  }
}
