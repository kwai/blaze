/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.joins.auron.plan

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.auron.plan.BroadcastLeft
import org.apache.spark.sql.execution.auron.plan.BroadcastRight
import org.apache.spark.sql.execution.auron.plan.BroadcastSide
import org.apache.spark.sql.execution.auron.plan.NativeBroadcastJoinBase
import org.apache.spark.sql.execution.joins.HashJoin

import org.apache.auron.sparkver

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

  @sparkver("3.1 / 3.2 / 3.3 / 3.4 / 3.5")
  override def buildSide: org.apache.spark.sql.catalyst.optimizer.BuildSide =
    broadcastSide match {
      case BroadcastLeft => org.apache.spark.sql.catalyst.optimizer.BuildLeft
      case BroadcastRight => org.apache.spark.sql.catalyst.optimizer.BuildRight
    }

  @sparkver("3.0")
  override val buildSide: org.apache.spark.sql.execution.joins.BuildSide = broadcastSide match {
    case BroadcastLeft => org.apache.spark.sql.execution.joins.BuildLeft
    case BroadcastRight => org.apache.spark.sql.execution.joins.BuildRight
  }

  @sparkver("3.1 / 3.2 / 3.3 / 3.4 / 3.5")
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

  override def rewriteKeyExprToLong(exprs: Seq[Expression]): Seq[Expression] =
    HashJoin.rewriteKeyExpr(exprs)

  @sparkver("3.1 / 3.2 / 3.3 / 3.4 / 3.5")
  override def supportCodegen: Boolean = false

  @sparkver("3.1 / 3.2 / 3.3 / 3.4 / 3.5")
  override def inputRDDs() = {
    throw new NotImplementedError("NativeBroadcastJoin dose not support codegen")
  }

  @sparkver("3.1 / 3.2 / 3.3 / 3.4 / 3.5")
  override protected def prepareRelation(
      ctx: org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext)
      : org.apache.spark.sql.execution.joins.HashedRelationInfo = {
    throw new NotImplementedError("NativeBroadcastJoin dose not support codegen")
  }

  @sparkver("3.2 / 3.3 / 3.4 / 3.5")
  override protected def withNewChildrenInternal(
      newLeft: SparkPlan,
      newRight: SparkPlan): SparkPlan =
    copy(left = newLeft, right = newRight)

  @sparkver("3.0 / 3.1")
  override def withNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
    copy(left = newChildren(0), right = newChildren(1))
}
