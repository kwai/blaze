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
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.blaze.MetricNode
import org.apache.spark.sql.blaze.NativeConverters
import org.apache.spark.sql.blaze.NativeRDD
import org.apache.spark.sql.blaze.NativeSupports
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.NullsFirst
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.TakeOrderedAndProjectExec
import org.apache.spark.OneToOneDependency
import org.apache.spark.sql.execution.adaptive.ShuffleQueryStage
import org.apache.spark.ShuffleDependency
import org.blaze.protobuf.PhysicalExprNode
import org.blaze.protobuf.PhysicalPlanNode
import org.blaze.protobuf.PhysicalSortExprNode
import org.blaze.protobuf.SortExecNode

case class NativeTakeOrderedExec(
    limit: Long,
    sortOrder: Seq[SortOrder],
    override val child: SparkPlan)
    extends UnaryExecNode
    with NativeSupports {

  override lazy val metrics: Map[String, SQLMetric] =
    NativeSupports.getDefaultNativeMetrics(sparkContext)

  override val output: Seq[Attribute] = child.output
  override val outputPartitioning: Partitioning = SinglePartition
  override val outputOrdering: Seq[SortOrder] = sortOrder

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

  override def executeCollect(): Array[InternalRow] = {
    val partial = ConvertToUnsafeRowExec(NativePartialTakeOrderedExec(child, sortOrder))
    val ord = new LazilyGeneratedOrdering(sortOrder, output)

    // all partitions are sorted, so perform a sorted-merge to achieve the result
    partial
      .execute()
      .mapPartitions(iter => Iterator.single(iter.toArray))
      .reduce {
        case (array1, array2) =>
          val result = ArrayBuffer[InternalRow]()
          var i = 0
          var j = 0

          while (result.length < limit && (i < array1.length || j < array2.length)) {
            0 match {
              case _ if i == array1.length =>
                result.append(array2(j))
                j += 1
              case _ if j == array2.length =>
                result.append(array1(i))
                i += 1
              case _ =>
                if (ord.compare(array1(i), array2(j)) <= 0) {
                  result.append(array1(i))
                  i += 1
                } else {
                  result.append(array2(j))
                  j += 1
                }
            }
          }
          result.toArray
      }
  }

  override def doExecuteNative(): NativeRDD = {
    val partial = NativePartialTakeOrderedExec(child, sortOrder)
    if (partial.outputPartitioning.numPartitions <= 1) {
      return NativeSupports.executeNative(partial)
    }

    // merge top-K from every children partitions into a single partition
    val shuffled = ArrowShuffleExchangeExec(SinglePartition, partial)
    val shuffledRDD = NativeSupports.executeNative(shuffled)

    // take top-K from the final partition
    new NativeRDD(
      sparkContext,
      metrics = MetricNode(metrics, shuffledRDD.metrics :: Nil),
      shuffledRDD.partitions,
      new OneToOneDependency(shuffledRDD) :: Nil,
      rddShuffleReadFull = false,
      (_, taskContext) => {
        val inputPartition = shuffledRDD.partitions(0)
        val nativeTakeOrderedExec = SortExecNode
          .newBuilder()
          .setInput(shuffledRDD.nativePlan(inputPartition, taskContext))
          .addAllExpr(nativeSortExprs.asJava)
          .setFetchLimit(limit)
          .build()
        PhysicalPlanNode.newBuilder().setSort(nativeTakeOrderedExec).build()
      },
      friendlyName = "NativeRDD.FinalTakeOrdered")
  }

  override def doCanonicalize(): SparkPlan =
    TakeOrderedAndProjectExec(limit.toInt, sortOrder, Nil, child).canonicalized

  case class NativePartialTakeOrderedExec(
      override val child: SparkPlan,
      sortOrder: Seq[SortOrder])
      extends UnaryExecNode
      with NativeSupports {

    override val output: Seq[Attribute] = child.output
    override val outputPartitioning: Partitioning = child.outputPartitioning

    override def doExecuteNative(): NativeRDD = {
      val inputRDD = NativeSupports.executeNative(child)
      new NativeRDD(
        sparkContext,
        metrics = MetricNode(Map(), inputRDD.metrics :: Nil),
        inputRDD.partitions,
        new OneToOneDependency(inputRDD) :: Nil,
        rddShuffleReadFull = false,
        (partition, taskContext) => {
          val inputPartition = inputRDD.partitions(partition.index)
          val nativeTakeOrderedExec = SortExecNode
            .newBuilder()
            .setInput(inputRDD.nativePlan(inputPartition, taskContext))
            .addAllExpr(nativeSortExprs.asJava)
            .setFetchLimit(limit)
            .build()
          PhysicalPlanNode.newBuilder().setSort(nativeTakeOrderedExec).build()
        },
        friendlyName = "NativeRDD.PartialTakeOrdered")
    }
  }

  override def withNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
    copy(child = newChildren.head)
}
