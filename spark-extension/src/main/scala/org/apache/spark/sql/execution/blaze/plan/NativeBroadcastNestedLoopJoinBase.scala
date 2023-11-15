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

import org.apache.spark.OneToOneDependency
import org.apache.spark.Partition
import org.apache.spark.sql.blaze.MetricNode
import org.apache.spark.sql.blaze.NativeConverters
import org.apache.spark.sql.blaze.NativeHelper
import org.apache.spark.sql.blaze.NativeRDD
import org.apache.spark.sql.blaze.NativeSupports
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.ExistenceJoin
import org.apache.spark.sql.catalyst.plans.FullOuter
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.InnerLike
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.LeftAnti
import org.apache.spark.sql.catalyst.plans.LeftExistence
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.catalyst.plans.LeftSemi
import org.apache.spark.sql.catalyst.plans.RightOuter
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.BinaryExecNode
import org.blaze.{protobuf => pb}

abstract class NativeBroadcastNestedLoopJoinBase(
    override val left: SparkPlan,
    override val right: SparkPlan,
    joinType: JoinType,
    condition: Option[Expression])
    extends BinaryExecNode
    with NativeSupports {

  override lazy val metrics: Map[String, SQLMetric] = Map(
    NativeHelper
      .getDefaultNativeMetrics(sparkContext)
      .filterKeys(Set("output_rows", "elapsed_compute"))
      .toSeq: _*)

  override def output: Seq[Attribute] = {
    joinType match {
      case _: InnerLike =>
        left.output ++ right.output
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case FullOuter =>
        left.output.map(_.withNullability(true)) ++ right.output.map(_.withNullability(true))
      case j: ExistenceJoin =>
        left.output :+ j.exists
      case LeftExistence(_) =>
        left.output
      case x =>
        throw new IllegalArgumentException(
          s"BroadcastNestedLoopJoin should not take $x as the JoinType")
    }
  }

  private def nativeJoinType = NativeConverters.convertJoinType(joinType)
  private def nativeJoinFilter =
    condition.map(NativeConverters.convertJoinFilter(_, left.output, right.output))

  // check whether native converting is supported
  nativeJoinType
  nativeJoinFilter

  private val probedSide = joinType match {
    case Inner | LeftOuter | LeftSemi | LeftAnti => "left"
    case RightOuter | FullOuter => "right"
    case other => s"NativeBroadcastNestedLoopJoin does not support join type $other"
  }

  override def doExecuteNative(): NativeRDD = {
    val leftRDD = NativeHelper.executeNative(left)
    val rightRDD = NativeHelper.executeNative(right)
    val nativeMetrics = MetricNode(metrics, leftRDD.metrics :: rightRDD.metrics :: Nil)
    val nativeJoinType = this.nativeJoinType
    val nativeJoinFilter = this.nativeJoinFilter
    val partitions = probedSide match {
      case "left" => leftRDD.partitions
      case "right" => rightRDD.partitions
    }

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      partitions,
      rddDependencies = probedSide match {
        case "left" => new OneToOneDependency(leftRDD) :: Nil
        case "right" => new OneToOneDependency(rightRDD) :: Nil
      },
      rightRDD.isShuffleReadFull,
      (partition, context) => {
        val partition0 = new Partition() {
          override def index: Int = 0
        }
        val (leftChild, rightChild) = probedSide match {
          case "left" =>
            (
              leftRDD.nativePlan(leftRDD.partitions(partition.index), context),
              rightRDD.nativePlan(partition0, context))
          case "right" =>
            (
              leftRDD.nativePlan(partition0, context),
              rightRDD.nativePlan(rightRDD.partitions(partition.index), context))
        }
        val bnlj = pb.BroadcastNestedLoopJoinExecNode
          .newBuilder()
          .setLeft(leftChild)
          .setRight(rightChild)
          .setJoinType(nativeJoinType)

        nativeJoinFilter.foreach(joinFilter => bnlj.setJoinFilter(joinFilter))
        pb.PhysicalPlanNode.newBuilder().setBroadcastNestedLoopJoin(bnlj).build()
      },
      friendlyName = "NativeRDD.BroadcastNestedLoopJoin")
  }
}
