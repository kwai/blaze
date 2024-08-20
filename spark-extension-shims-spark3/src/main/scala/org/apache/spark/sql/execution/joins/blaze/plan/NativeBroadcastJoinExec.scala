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
package org.apache.spark.sql.execution.joins.blaze.plan

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.blaze.plan.BroadcastLeft
import org.apache.spark.sql.execution.blaze.plan.BroadcastRight
import org.apache.spark.sql.execution.blaze.plan.BroadcastSide
import org.apache.spark.sql.execution.blaze.plan.NativeBroadcastJoinBase
import org.apache.spark.sql.execution.joins.HashJoin

import com.thoughtworks.enableIf

case class NativeBroadcastJoinExec(
    override val left: SparkPlan,
    override val right: SparkPlan,
    override val outputPartitioning: Partitioning,
    override val leftKeys: Seq[Expression],
    override val rightKeys: Seq[Expression],
    override val joinType: JoinType,
    broadcastSide: BroadcastSide)
    extends NativeBroadcastJoinBase(
      left,
      right,
      outputPartitioning,
      leftKeys,
      rightKeys,
      joinType,
      broadcastSide)
    with HashJoin {

  override val condition: Option[Expression] = None

  @enableIf(
    Seq("spark313", "spark320", "spark324", "spark333", "spark351").contains(
      System.getProperty("blaze.shim")))
  override def buildSide: org.apache.spark.sql.catalyst.optimizer.BuildSide =
    broadcastSide match {
      case BroadcastLeft => org.apache.spark.sql.catalyst.optimizer.BuildLeft
      case BroadcastRight => org.apache.spark.sql.catalyst.optimizer.BuildRight
    }

  @enableIf(Seq("spark303").contains(System.getProperty("blaze.shim")))
  override val buildSide: org.apache.spark.sql.execution.joins.BuildSide = broadcastSide match {
    case BroadcastLeft => org.apache.spark.sql.execution.joins.BuildLeft
    case BroadcastRight => org.apache.spark.sql.execution.joins.BuildRight
  }

  @enableIf(
    Seq("spark313", "spark320", "spark324", "spark333", "spark351").contains(
      System.getProperty("blaze.shim")))
  override def requiredChildDistribution = {
    import org.apache.spark.sql.catalyst.plans.physical.BroadcastDistribution
    import org.apache.spark.sql.catalyst.plans.physical.UnspecifiedDistribution
    import org.apache.spark.sql.execution.joins.HashedRelationBroadcastMode

    def mode = HashedRelationBroadcastMode(buildBoundKeys, isNullAware = false)
    broadcastSide match {
      case BroadcastLeft =>
        BroadcastDistribution(mode) :: UnspecifiedDistribution :: Nil
      case BroadcastRight =>
        UnspecifiedDistribution :: BroadcastDistribution(mode) :: Nil
    }
  }

  @enableIf(
    Seq("spark313", "spark320", "spark324", "spark333", "spark351").contains(
      System.getProperty("blaze.shim")))
  override def supportCodegen: Boolean = false

  @enableIf(
    Seq("spark313", "spark320", "spark324", "spark333", "spark351").contains(
      System.getProperty("blaze.shim")))
  override def inputRDDs() = {
    throw new NotImplementedError("NativeBroadcastJoin dose not support codegen")
  }

  @enableIf(
    Seq("spark313", "spark320", "spark324", "spark333", "spark351").contains(
      System.getProperty("blaze.shim")))
  override protected def prepareRelation(
      ctx: org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext)
      : org.apache.spark.sql.execution.joins.HashedRelationInfo = {
    throw new NotImplementedError("NativeBroadcastJoin dose not support codegen")
  }

  @enableIf(
    Seq("spark320", "spark324", "spark333", "spark351").contains(
      System.getProperty("blaze.shim")))
  override protected def withNewChildrenInternal(
      newLeft: SparkPlan,
      newRight: SparkPlan): SparkPlan =
    copy(left = newLeft, right = newRight)

  @enableIf(Seq("spark303", "spark313").contains(System.getProperty("blaze.shim")))
  override def withNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
    copy(left = newChildren(0), right = newChildren(1))
}
