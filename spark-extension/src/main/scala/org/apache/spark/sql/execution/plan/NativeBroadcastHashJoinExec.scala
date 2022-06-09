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

import org.apache.spark.sql.blaze.MetricNode
import org.apache.spark.sql.blaze.NativeConverters
import org.apache.spark.sql.blaze.NativeRDD
import org.apache.spark.sql.blaze.NativeSupports
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.BinaryExecNode
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.execution.joins.BuildLeft
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.Partition
import org.apache.spark.sql.catalyst.plans.ExistenceJoin
import org.apache.spark.sql.catalyst.plans.InnerLike
import org.apache.spark.sql.catalyst.plans.LeftExistence
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.catalyst.plans.RightOuter
import org.blaze.protobuf.HashJoinExecNode
import org.blaze.protobuf.JoinOn
import org.blaze.protobuf.PhysicalPlanNode

case class NativeBroadcastHashJoinExec(
    override val left: SparkPlan,
    override val right: SparkPlan,
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    override val outputPartitioning: Partitioning,
    override val outputOrdering: Seq[SortOrder],
    joinType: JoinType)
    extends BinaryExecNode
    with NativeSupports {

  override def output: Seq[Attribute] = {
    joinType match {
      case _: InnerLike =>
        left.output ++ right.output
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case j: ExistenceJoin =>
        left.output :+ j.exists
      case LeftExistence(_) =>
        left.output
      case x =>
        throw new IllegalArgumentException(s"HashJoin should not take $x as the JoinType")
    }
  }
  override lazy val metrics: Map[String, SQLMetric] =
    NativeSupports.getDefaultNativeMetrics(sparkContext)

  private val nativeJoinOn = leftKeys.zip(rightKeys).map {
    case (leftKey, rightKey) =>
      val leftColumn = NativeConverters.convertExpr(leftKey).getColumn match {
        case column if column.getName.isEmpty =>
          throw new NotImplementedError(s"BHJ leftKey is not column: ${leftKey}")
        case column => column
      }
      val rightColumn = NativeConverters.convertExpr(rightKey).getColumn match {
        case column if column.getName.isEmpty =>
          throw new NotImplementedError(s"BHJ leftKey is not column: ${rightKey}")
        case column => column
      }
      JoinOn
        .newBuilder()
        .setLeft(leftColumn)
        .setRight(rightColumn)
        .build()
  }

  private val nativeJoinType = NativeConverters.convertJoinType(joinType)

  override def doExecuteNative(): NativeRDD = {
    val leftRDD = NativeSupports.executeNative(left)
    val rightRDD = NativeSupports.executeNative(right)
    val nativeMetrics = MetricNode(metrics, Seq(leftRDD.metrics, rightRDD.metrics))
    val partitions = rightRDD.partitions
    val dependencies = rightRDD.dependencies

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      partitions,
      dependencies,
      (partition, context) => {
        val partition0 = new Partition() {
          override def index: Int = 0
        }
        val rightPartition = rightRDD.partitions(partition.index)
        val leftChild = leftRDD.nativePlan(partition0, context)
        val rightChild = rightRDD.nativePlan(rightPartition, context)

        val hashJoinExec = HashJoinExecNode
          .newBuilder()
          .setLeft(leftChild)
          .setRight(rightChild)
          .setJoinType(nativeJoinType)
          .addAllOn(nativeJoinOn.asJava)
          .setNullEqualsNull(false)
        PhysicalPlanNode.newBuilder().setHashJoin(hashJoinExec).build()
      })
  }

  override def doCanonicalize(): SparkPlan =
    BroadcastHashJoinExec(
      leftKeys,
      rightKeys,
      joinType,
      buildSide = BuildLeft,
      condition = None,
      left,
      right).canonicalized
}
