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

package org.apache.spark.sql.execution.plan

import scala.collection.JavaConverters._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.blaze.MetricNode
import org.apache.spark.sql.blaze.NativeConverters
import org.apache.spark.sql.blaze.NativeRDD
import org.apache.spark.sql.blaze.NativeSupports
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.plans.RightOuter
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.OneToOneDependency
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.BinaryExecNode
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.blaze.protobuf.JoinOn
import org.blaze.protobuf.PhysicalPlanNode
import org.blaze.protobuf.SortMergeJoinExecNode
import org.blaze.protobuf.SortOptions

case class NativeSortMergeJoinExec(
    override val left: SparkPlan,
    override val right: SparkPlan,
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    override val output: Seq[Attribute],
    override val outputPartitioning: Partitioning,
    override val outputOrdering: Seq[SortOrder],
    joinType: JoinType)
    extends BinaryExecNode
    with NativeSupports {

  override lazy val metrics: Map[String, SQLMetric] =
    NativeSupports.getDefaultNativeMetrics(sparkContext)

  private val nativeJoinOn = leftKeys.zip(rightKeys).map {
    case (leftKey, rightKey) =>
      val leftColumn = NativeConverters.convertExpr(leftKey).getColumn match {
        case column if column.getName.isEmpty =>
          throw new NotImplementedError(s"SMJ leftKey is not column: ${leftKey}")
        case column => column
      }
      val rightColumn = NativeConverters.convertExpr(rightKey).getColumn match {
        case column if column.getName.isEmpty =>
          throw new NotImplementedError(s"SMJ leftKey is not column: ${rightKey}")
        case column => column
      }
      JoinOn
        .newBuilder()
        .setLeft(leftColumn)
        .setRight(rightColumn)
        .build()
  }

  private val nativeSortOptions = nativeJoinOn.map(_ => {
    SortOptions
      .newBuilder()
      .setAsc(true)
      .setNullsFirst(true)
      .build()
  })

  private val nativeJoinType = NativeConverters.convertJoinType(joinType)

  override def doExecuteNative(): NativeRDD = {
    val leftRDD = NativeSupports.executeNative(left)
    val rightRDD = NativeSupports.executeNative(right)
    val nativeMetrics = MetricNode(metrics, Seq(leftRDD.metrics, rightRDD.metrics))

    val partitions = if (joinType != RightOuter) {
      leftRDD.partitions
    } else {
      rightRDD.partitions
    }

    val dependencies = Seq(
      new OneToOneDependency[InternalRow](leftRDD.asInstanceOf[RDD[InternalRow]]),
      new OneToOneDependency[InternalRow](rightRDD.asInstanceOf[RDD[InternalRow]]))
    new NativeRDD(
      sparkContext,
      nativeMetrics,
      partitions,
      dependencies,
      (partition, taskContext) => {
        val leftPartition = leftRDD.partitions(partition.index)
        val leftChild = leftRDD.nativePlan(leftPartition, taskContext)

        val rightPartition = rightRDD.partitions(partition.index)
        val rightChild = rightRDD.nativePlan(rightPartition, taskContext)

        val sortMergeJoinExec = SortMergeJoinExecNode
          .newBuilder()
          .setLeft(leftChild)
          .setRight(rightChild)
          .setJoinType(nativeJoinType)
          .addAllOn(nativeJoinOn.asJava)
          .addAllSortOptions(nativeSortOptions.asJava)
          .setNullEqualsNull(false)
        PhysicalPlanNode.newBuilder().setSortMergeJoin(sortMergeJoinExec).build()
      })
  }

  override def doCanonicalize(): SparkPlan =
    SortMergeJoinExec(
      leftKeys,
      rightKeys,
      joinType,
      condition = None,
      left,
      right,
      isSkewJoin = false).canonicalized
}
