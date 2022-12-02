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

import org.apache.spark.sql.blaze.MetricNode
import org.apache.spark.sql.blaze.NativeConverters
import org.apache.spark.sql.blaze.NativeRDD
import org.apache.spark.sql.blaze.NativeHelper
import org.apache.spark.sql.catalyst.plans.ExistenceJoin
import org.apache.spark.sql.catalyst.plans.InnerLike
import org.apache.spark.sql.catalyst.plans.LeftExistence
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.catalyst.plans.RightOuter
import org.apache.spark.OneToOneDependency
import org.blaze.protobuf.ColumnIndex
import org.blaze.protobuf.HashJoinExecNode
import org.blaze.protobuf.JoinFilter
import org.blaze.protobuf.JoinOn
import org.blaze.protobuf.JoinSide
import org.blaze.protobuf.PhysicalPlanNode

import org.apache.spark.sql.blaze.NativeSupports

case class NativeBroadcastHashJoinExec(
    override val left: SparkPlan,
    override val right: SparkPlan,
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinFilter: Option[Expression],
    joinType: JoinType)
    extends BinaryExecNode
    with NativeSupports {

  override val outputPartitioning: Partitioning = {
    right.outputPartitioning // right side is always the streamed plan
  }

  override val outputOrdering: Seq[SortOrder] = Nil

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

  override lazy val metrics: Map[String, SQLMetric] = Map(
    NativeHelper
      .getDefaultNativeMetrics(sparkContext)
      .filterKeys(
        Set("output_rows", "output_batches", "input_rows", "input_batches", "join_time"))
      .toSeq: _*)

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

  private val nativeJoinFilter = joinFilter.map { filterExpr =>
    val schema = filterExpr.references.toSeq
    val columnIndices = ArrayBuffer[ColumnIndex]()
    for (attr <- schema) {
      attr.exprId match {
        case exprId if left.output.exists(_.exprId == exprId) =>
          columnIndices += ColumnIndex
            .newBuilder()
            .setSide(JoinSide.LEFT_SIDE)
            .setIndex(left.output.indexWhere(_.exprId == attr.exprId))
            .build()
        case exprId if right.output.exists(_.exprId == exprId) =>
          columnIndices += ColumnIndex
            .newBuilder()
            .setSide(JoinSide.RIGHT_SIDE)
            .setIndex(right.output.indexWhere(_.exprId == attr.exprId))
            .build()
        case _ =>
          throw new NotImplementedError(s"unsupported join filter: $filterExpr")
      }
    }
    JoinFilter
      .newBuilder()
      .setExpression(NativeConverters.convertExpr(filterExpr))
      .setSchema(Util.getNativeSchema(schema))
      .addAllColumnIndices(columnIndices.asJava)
      .build()
  }

  private val nativeJoinType = NativeConverters.convertJoinType(joinType)

  override def doExecuteNative(): NativeRDD = {
    val leftRDD = NativeHelper.executeNative(left)
    val rightRDD = NativeHelper.executeNative(right)
    logInfo(s"leftRDD is: ${leftRDD}")
    logInfo(s"rightRDD is: $rightRDD")
    val nativeMetrics = MetricNode(metrics, leftRDD.metrics :: rightRDD.metrics :: Nil)
    val partitions = rightRDD.partitions

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      partitions,
      rddDependencies = new OneToOneDependency(rightRDD) :: Nil,
      rightRDD.shuffleReadFull,
      (partition, context) => {
        val partition0 = new Partition() {
          override def index: Int = 0
        }
        val rightPartition = rightRDD.partitions(partition.index)
        val leftChild = leftRDD.nativePlan(partition0, context)
        val rightChild = rightRDD.nativePlan(rightPartition, context)
        logInfo(s"leftChild is: ${leftChild}")
        logInfo(s"rightChild is: $rightChild")

        val hashJoinExec = HashJoinExecNode
          .newBuilder()
          .setLeft(leftChild)
          .setRight(rightChild)
          .setJoinType(nativeJoinType)
          .addAllOn(nativeJoinOn.asJava)
          .setNullEqualsNull(false)

        nativeJoinFilter.foreach(joinFilter => hashJoinExec.setFilter(joinFilter))
        PhysicalPlanNode.newBuilder().setHashJoin(hashJoinExec).build()
      },
      friendlyName = "NativeRDD.BroadcastHashJoin")
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

  override def withNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
    copy(left = newChildren(0), right = newChildren(1))
}
