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
import org.apache.spark.sql.blaze.MetricNode
import org.apache.spark.sql.blaze.NativeConverters
import org.apache.spark.sql.blaze.NativeHelper
import org.apache.spark.sql.blaze.NativeRDD
import org.apache.spark.sql.blaze.NativeSupports
import org.apache.spark.sql.blaze.Shims
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.BinaryExecNode
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins.HashedRelationBroadcastMode
import org.apache.spark.sql.types.LongType
import org.blaze.{protobuf => pb}
import org.blaze.protobuf.JoinOn

abstract class NativeBroadcastJoinBase(
    override val left: SparkPlan,
    override val right: SparkPlan,
    override val outputPartitioning: Partitioning,
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    broadcastSide: BroadcastSide)
    extends BinaryExecNode
    with NativeSupports {

  override lazy val metrics: Map[String, SQLMetric] = SortedMap[String, SQLMetric]() ++ Map(
    NativeHelper
      .getDefaultNativeMetrics(sparkContext)
      .toSeq: _*)

  private val isLongHashRelation = {
    val baseBroadcast = broadcastSide match {
      case BroadcastLeft => Shims.get.getUnderlyingBroadcast(left)
      case BroadcastRight => Shims.get.getUnderlyingBroadcast(right)
    }
    val mode = baseBroadcast match {
      case b: BroadcastExchangeExec => b.mode
      case b: NativeBroadcastExchangeBase => b.broadcastMode
    }
    mode match {
      case mode: HashedRelationBroadcastMode
          if mode.key.length == 1 && mode.key.head.dataType == LongType =>
        true
      case _ => false
    }
  }

  private def nativeSchema = Util.getNativeSchema(output)

  private def nativeJoinOn = leftKeys.zip(rightKeys).map { case (leftKey, rightKey) =>
    val leftKeyExpr = leftKey match {
      case k if !isLongHashRelation || k.dataType == LongType => k
      case k => Cast(k, LongType)
    }
    val rightKeyExpr = rightKey match {
      case k if !isLongHashRelation || k.dataType == LongType => k
      case k => Cast(k, LongType)
    }
    JoinOn
      .newBuilder()
      .setLeft(NativeConverters.convertExpr(leftKeyExpr))
      .setRight(NativeConverters.convertExpr(rightKeyExpr))
      .build()
  }

  private def nativeJoinType = NativeConverters.convertJoinType(joinType)

  private def nativeBroadcastSide = broadcastSide match {
    case BroadcastLeft => pb.JoinSide.LEFT_SIDE
    case BroadcastRight => pb.JoinSide.RIGHT_SIDE
  }

  // check whether native converting is supported
  nativeSchema
  nativeJoinType
  nativeJoinOn
  nativeBroadcastSide

  override def doExecuteNative(): NativeRDD = {
    val leftRDD = NativeHelper.executeNative(left)
    val rightRDD = NativeHelper.executeNative(right)
    val nativeMetrics = MetricNode(metrics, leftRDD.metrics :: rightRDD.metrics :: Nil)
    val nativeSchema = this.nativeSchema
    val nativeJoinType = this.nativeJoinType
    val nativeJoinOn = this.nativeJoinOn

    val (probedRDD, builtRDD) = broadcastSide match {
      case BroadcastLeft => (rightRDD, leftRDD)
      case BroadcastRight => (leftRDD, rightRDD)
    }

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      probedRDD.partitions,
      rddDependencies = new OneToOneDependency(probedRDD) :: Nil,
      probedRDD.isShuffleReadFull,
      (partition, context) => {
        val partition0 = new Partition() {
          override def index: Int = 0
        }
        val (leftChild, rightChild) = broadcastSide match {
          case BroadcastLeft =>
            (
              leftRDD.nativePlan(partition0, context),
              rightRDD.nativePlan(rightRDD.partitions(partition.index), context))
          case BroadcastRight =>
            (
              leftRDD.nativePlan(leftRDD.partitions(partition.index), context),
              rightRDD.nativePlan(partition0, context))
        }
        val cachedBuildHashMapId = s"bhm_stage${context.stageId}_rdd${builtRDD.id}"

        val broadcastJoinExec = pb.BroadcastJoinExecNode
          .newBuilder()
          .setSchema(nativeSchema)
          .setLeft(leftChild)
          .setRight(rightChild)
          .setJoinType(nativeJoinType)
          .setBroadcastSide(nativeBroadcastSide)
          .setCachedBuildHashMapId(cachedBuildHashMapId)
          .addAllOn(nativeJoinOn.asJava)

        pb.PhysicalPlanNode.newBuilder().setBroadcastJoin(broadcastJoinExec).build()
      },
      friendlyName = "NativeRDD.BroadcastJoin")
  }
}

class BroadcastSide {}
case object BroadcastLeft extends BroadcastSide {}
case object BroadcastRight extends BroadcastSide {}
