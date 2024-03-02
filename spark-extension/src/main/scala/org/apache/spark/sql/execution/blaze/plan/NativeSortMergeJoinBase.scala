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

import org.apache.spark.sql.catalyst.plans.RightOuter
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.OneToOneDependency
import org.apache.spark.sql.blaze.BlazeConf
import org.apache.spark.sql.blaze.MetricNode
import org.apache.spark.sql.blaze.NativeConverters
import org.apache.spark.sql.blaze.NativeHelper
import org.apache.spark.sql.blaze.NativeRDD
import org.apache.spark.sql.blaze.NativeSupports
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.ExistenceJoin
import org.apache.spark.sql.catalyst.plans.InnerLike
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.LeftAnti
import org.apache.spark.sql.catalyst.plans.LeftSemi
import org.apache.spark.sql.execution.BinaryExecNode
import org.blaze.protobuf.JoinOn
import org.blaze.protobuf.PhysicalPlanNode
import org.blaze.protobuf.SortMergeJoinExecNode
import org.blaze.protobuf.SortOptions

abstract class NativeSortMergeJoinBase(
    override val left: SparkPlan,
    override val right: SparkPlan,
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression])
    extends BinaryExecNode
    with NativeSupports {

  assert(
    (joinType != LeftSemi && joinType != LeftAnti) || condition.isEmpty,
    "Semi/Anti join with filter is not supported yet")

  assert(
    BlazeConf.SMJ_INEQUALITY_JOIN_ENABLE.booleanConf() || condition.isEmpty,
    "inequality sort-merge join is not enabled")

  override lazy val metrics: Map[String, SQLMetric] = SortedMap[String, SQLMetric]() ++ Map(
    NativeHelper
      .getDefaultNativeMetrics(sparkContext)
      .filterKeys(
        Set(
          "stage_id",
          "output_rows",
          "elapsed_compute",
          "input_batch_count",
          "input_batch_mem_size",
          "input_row_count"))
      .toSeq: _*)

  private def nativeJoinOn = leftKeys.zip(rightKeys).map { case (leftKey, rightKey) =>
    val leftColumn = NativeConverters.convertExpr(leftKey).getColumn match {
      case column if column.getName.isEmpty =>
        throw new NotImplementedError(s"SMJ leftKey is not column: ${leftKey}")
      case column => column
    }
    val rightColumn = NativeConverters.convertExpr(rightKey).getColumn match {
      case column if column.getName.isEmpty =>
        throw new NotImplementedError(s"SMJ rightKey is not column: ${rightKey}")
      case column => column
    }
    JoinOn
      .newBuilder()
      .setLeft(leftColumn)
      .setRight(rightColumn)
      .build()
  }

  private def nativeSortOptions = nativeJoinOn.map(_ => {
    SortOptions
      .newBuilder()
      .setAsc(true)
      .setNullsFirst(true)
      .build()
  })

  private def nativeJoinType = NativeConverters.convertJoinType(joinType)

  private def nativeJoinFilter =
    condition.map(NativeConverters.convertJoinFilter(_, left.output, right.output))

  // check whether native converting is supported
  nativeSortOptions
  nativeJoinOn
  nativeJoinType
  nativeJoinFilter

  override def doExecuteNative(): NativeRDD = {
    val leftRDD = NativeHelper.executeNative(left)
    val rightRDD = NativeHelper.executeNative(right)
    val nativeMetrics = MetricNode(metrics, leftRDD.metrics :: rightRDD.metrics :: Nil)
    val nativeSortOptions = this.nativeSortOptions
    val nativeJoinOn = this.nativeJoinOn
    val nativeJoinType = this.nativeJoinType
    val nativeJoinFilter = this.nativeJoinFilter

    val partitions = if (joinType != RightOuter) {
      leftRDD.partitions
    } else {
      rightRDD.partitions
    }
    val dependencies = Seq(new OneToOneDependency(leftRDD), new OneToOneDependency(rightRDD))
    val isShuffleReadFull = joinType match {
      case _: InnerLike =>
        logInfo("SortMergeJoin Inner mark shuffleReadFull = false")
        false
      case LeftAnti | LeftSemi =>
        logInfo("SortMergeJoin LeftAnti|LeftSemi mark shuffleReadFull = false")
        false
      case _: ExistenceJoin =>
        logInfo("SortMergeJoin ExistenceJoin mark shuffleReadFull = false")
        false
      case _ => leftRDD.isShuffleReadFull && rightRDD.isShuffleReadFull
    }

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      partitions,
      dependencies,
      isShuffleReadFull,
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

        nativeJoinFilter.foreach(joinFilter => sortMergeJoinExec.setJoinFilter(joinFilter))
        PhysicalPlanNode.newBuilder().setSortMergeJoin(sortMergeJoinExec).build()
      },
      friendlyName = "NativeRDD.SortMergeJoin")
  }
}
