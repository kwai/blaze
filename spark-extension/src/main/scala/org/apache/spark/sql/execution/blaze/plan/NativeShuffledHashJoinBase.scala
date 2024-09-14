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
import org.apache.spark.sql.blaze.MetricNode
import org.apache.spark.sql.blaze.NativeConverters
import org.apache.spark.sql.blaze.NativeHelper
import org.apache.spark.sql.blaze.NativeRDD
import org.apache.spark.sql.blaze.NativeSupports
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.BinaryExecNode
import org.blaze.{protobuf => pb}

abstract class NativeShuffledHashJoinBase(
    override val left: SparkPlan,
    override val right: SparkPlan,
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide)
    extends BinaryExecNode
    with NativeSupports {

  override lazy val metrics: Map[String, SQLMetric] = SortedMap[String, SQLMetric]() ++ Map(
    NativeHelper
      .getDefaultNativeMetrics(sparkContext)
      .filterKeys(Set(
        "stage_id",
        "output_rows",
        "elapsed_compute",
        "build_hash_map_time",
        "probed_side_hash_time",
        "probed_side_search_time",
        "probed_side_compare_time",
        "build_output_time",
        "input_batch_count",
        "input_batch_mem_size",
        "input_row_count"))
      .toSeq: _*)

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    requiredOrders(leftKeys) :: requiredOrders(rightKeys) :: Nil

  private def requiredOrders(keys: Seq[Expression]): Seq[SortOrder] = {
    // This must be ascending in order to agree with the `keyOrdering` defined in `doExecute()`.
    keys.map(SortOrder(_, Ascending))
  }

  private def nativeSchema = Util.getNativeSchema(output)

  private def nativeJoinOn = leftKeys.zip(rightKeys).map { case (leftKey, rightKey) =>
    val leftKeyExpr = NativeConverters.convertExpr(leftKey)
    val rightKeyExpr = NativeConverters.convertExpr(rightKey)
    pb.JoinOn
      .newBuilder()
      .setLeft(leftKeyExpr)
      .setRight(rightKeyExpr)
      .build()
  }

  private def nativeJoinType = NativeConverters.convertJoinType(joinType)

  private def nativeBuildSide = buildSide match {
    case BuildLeft => pb.JoinSide.LEFT_SIDE
    case BuildRight => pb.JoinSide.RIGHT_SIDE
  }
  // check whether native converting is supported
  nativeSchema
  nativeJoinOn
  nativeJoinType
  nativeBuildSide

  override def doExecuteNative(): NativeRDD = {
    val leftRDD = NativeHelper.executeNative(left)
    val rightRDD = NativeHelper.executeNative(right)
    val nativeMetrics = MetricNode(metrics, leftRDD.metrics :: rightRDD.metrics :: Nil)
    val nativeJoinOn = this.nativeJoinOn
    val nativeJoinType = this.nativeJoinType
    val nativeBuildSide = this.nativeBuildSide

    val partitions = if (joinType != RightOuter) {
      leftRDD.partitions
    } else {
      rightRDD.partitions
    }
    val dependencies = Seq(new OneToOneDependency(leftRDD), new OneToOneDependency(rightRDD))

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      partitions,
      dependencies,
      leftRDD.isShuffleReadFull && rightRDD.isShuffleReadFull,
      (partition, taskContext) => {
        val leftPartition = leftRDD.partitions(partition.index)
        val leftChild = leftRDD.nativePlan(leftPartition, taskContext)

        val rightPartition = rightRDD.partitions(partition.index)
        val rightChild = rightRDD.nativePlan(rightPartition, taskContext)

        val hashJoinExec = pb.HashJoinExecNode
          .newBuilder()
          .setSchema(nativeSchema)
          .setLeft(leftChild)
          .setRight(rightChild)
          .setJoinType(nativeJoinType)
          .addAllOn(nativeJoinOn.asJava)
          .setBuildSide(nativeBuildSide)
        pb.PhysicalPlanNode.newBuilder().setHashJoin(hashJoinExec).build()
      },
      friendlyName = "NativeRDD.ShuffledHashJoin")
  }
}

class BuildSide {}
case object BuildLeft extends BuildSide {}
case object BuildRight extends BuildSide {}
