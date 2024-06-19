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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.optimizer.BuildLeft
import org.apache.spark.sql.catalyst.optimizer.BuildRight
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.catalyst.plans.physical.BroadcastDistribution
import org.apache.spark.sql.catalyst.plans.physical.Distribution
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.plans.physical.UnspecifiedDistribution
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.blaze.plan.BroadcastLeft
import org.apache.spark.sql.execution.blaze.plan.BroadcastRight
import org.apache.spark.sql.execution.blaze.plan.BroadcastSide
import org.apache.spark.sql.execution.blaze.plan.NativeBroadcastJoinBase
import org.apache.spark.sql.execution.joins.HashedRelationBroadcastMode
import org.apache.spark.sql.execution.joins.HashedRelationInfo
import org.apache.spark.sql.execution.joins.HashJoin

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

  override def condition: Option[Expression] = None

  override def requiredChildDistribution: Seq[Distribution] = {
    val mode = HashedRelationBroadcastMode(buildBoundKeys, isNullAware = false)
    BroadcastDistribution(mode) :: UnspecifiedDistribution :: Nil
  }

  override def supportCodegen: Boolean = false

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    throw new NotImplementedError("NativeBroadcastJoin dose not support codegen")
  }

  override protected def prepareRelation(ctx: CodegenContext): HashedRelationInfo = {
    throw new NotImplementedError("NativeBroadcastJoin dose not support codegen")
  }

  override def buildSide: BuildSide = broadcastSide match {
    case BroadcastLeft => BuildLeft
    case BroadcastRight => BuildRight
  }

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan,
      newRight: SparkPlan): SparkPlan =
    copy(left = newLeft, right = newRight)
}
