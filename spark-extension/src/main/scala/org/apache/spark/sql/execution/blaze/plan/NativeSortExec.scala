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

import org.apache.spark.sql.blaze.{MetricNode, NativeConverters, NativeRDD, NativeSupports}
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, NullsFirst, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{
  Distribution,
  OrderedDistribution,
  Partitioning,
  UnspecifiedDistribution
}
import org.apache.spark.sql.execution.{SortExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.blaze.protobuf.{PhysicalExprNode, PhysicalPlanNode, PhysicalSortExprNode, SortExecNode}

import scala.collection.JavaConverters._

case class NativeSortExec(
    sortOrder: Seq[SortOrder],
    global: Boolean,
    override val child: SparkPlan)
    extends UnaryExecNode
    with NativeSupports {

  override lazy val metrics: Map[String, SQLMetric] =
    NativeSupports.getDefaultNativeMetrics(sparkContext)

  override def output: Seq[Attribute] = child.output

  override def outputOrdering: Seq[SortOrder] = sortOrder

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] =
    if (global) {
      OrderedDistribution(sortOrder) :: Nil
    } else {
      UnspecifiedDistribution :: Nil
    }

  private val nativeSortExprs = sortOrder.map { sortOrder =>
    PhysicalExprNode
      .newBuilder()
      .setSort(
        PhysicalSortExprNode
          .newBuilder()
          .setExpr(NativeConverters.convertExpr(sortOrder.child))
          .setAsc(sortOrder.direction == Ascending)
          .setNullsFirst(sortOrder.nullOrdering == NullsFirst)
          .build())
      .build()
  }

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
        val inputPartition = inputRDD.partitions(partition.index)
        val nativeSortExec = SortExecNode
          .newBuilder()
          .setInput(inputRDD.nativePlan(inputPartition, taskContext))
          .addAllExpr(nativeSortExprs.asJava)
          .build()
        PhysicalPlanNode.newBuilder().setSort(nativeSortExec).build()
      },
      friendlyName = "NativeRDD.Sort")
  }

  override def doCanonicalize(): SparkPlan =
    SortExec(sortOrder, global, child, testSpillFrequency = 0).canonicalized
}
