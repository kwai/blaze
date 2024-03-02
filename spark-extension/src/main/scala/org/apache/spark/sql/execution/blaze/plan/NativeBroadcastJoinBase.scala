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
import org.apache.spark.Partition
import org.apache.spark.sql.blaze.BlazeConf
import org.apache.spark.sql.blaze.MetricNode
import org.apache.spark.sql.blaze.NativeConverters
import org.apache.spark.sql.blaze.NativeHelper
import org.apache.spark.sql.blaze.NativeRDD
import org.apache.spark.sql.blaze.NativeSupports
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.LeftAnti
import org.apache.spark.sql.catalyst.plans.LeftSemi
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.BinaryExecNode
import org.blaze.{protobuf => pb}

abstract class NativeBroadcastJoinBase(
    override val left: SparkPlan,
    override val right: SparkPlan,
    override val outputPartitioning: Partitioning,
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
    !BlazeConf.BHJ_FALLBACKS_TO_SMJ_ENABLE.booleanConf() || BlazeConf.SMJ_INEQUALITY_JOIN_ENABLE
      .booleanConf() || condition.isEmpty,
    "Join filter is not supported when BhjFallbacksToSmj and SmjInequalityJoin both enabled")

  override lazy val metrics: Map[String, SQLMetric] = SortedMap[String, SQLMetric]() ++ Map(
    NativeHelper
      .getDefaultNativeMetrics(sparkContext)
      .toSeq: _*)

  private def nativeJoinOn = leftKeys.zip(rightKeys).map { case (leftKey, rightKey) =>
    val leftColumn = NativeConverters.convertExpr(leftKey).getColumn match {
      case column if column.getName.isEmpty =>
        throw new NotImplementedError(s"BHJ leftKey is not column: ${leftKey}")
      case column => column
    }
    val rightColumn = NativeConverters.convertExpr(rightKey).getColumn match {
      case column if column.getName.isEmpty =>
        throw new NotImplementedError(s"BHJ rightKey is not column: ${rightKey}")
      case column => column
    }
    pb.JoinOn
      .newBuilder()
      .setLeft(leftColumn)
      .setRight(rightColumn)
      .build()
  }

  private def nativeJoinType = NativeConverters.convertJoinType(joinType)

  private def nativeJoinFilter =
    condition.map(NativeConverters.convertJoinFilter(_, left.output, right.output))

  // check whether native converting is supported
  nativeJoinType
  nativeJoinFilter

  override def doExecuteNative(): NativeRDD = {
    val leftRDD = NativeHelper.executeNative(left)
    val rightRDD = NativeHelper.executeNative(right)
    val nativeMetrics = MetricNode(metrics, leftRDD.metrics :: rightRDD.metrics :: Nil)
    val nativeJoinType = this.nativeJoinType
    val nativeJoinOn = this.nativeJoinOn
    val nativeJoinFilter = this.nativeJoinFilter
    val partitions = rightRDD.partitions

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      partitions,
      rddDependencies = new OneToOneDependency(rightRDD) :: Nil,
      rightRDD.isShuffleReadFull,
      (partition, context) => {
        val partition0 = new Partition() {
          override def index: Int = 0
        }
        val leftChild = leftRDD.nativePlan(partition0, context)
        val rightChild = rightRDD.nativePlan(rightRDD.partitions(partition.index), context)
        val broadcastJoinExec = pb.BroadcastJoinExecNode
          .newBuilder()
          .setLeft(leftChild)
          .setRight(rightChild)
          .setJoinType(nativeJoinType)
          .addAllOn(nativeJoinOn.asJava)

        nativeJoinFilter.foreach(joinFilter => broadcastJoinExec.setJoinFilter(joinFilter))
        pb.PhysicalPlanNode.newBuilder().setBroadcastJoin(broadcastJoinExec).build()
      },
      friendlyName = "NativeRDD.BroadcastJoin")
  }
}
