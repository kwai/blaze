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
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.auron.plan.BuildSide
import org.apache.spark.sql.execution.auron.plan.NativeShuffledHashJoinBase
import org.apache.spark.sql.execution.joins.HashJoin
import org.apache.auron.sparkver

case object NativeShuffledHashJoinExecProvider {

  @sparkver("3.2 / 3.3 / 3.4 / 3.5")
  def provide(
      left: SparkPlan,
      right: SparkPlan,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      buildSide: BuildSide,
      isSkewJoin: Boolean): NativeShuffledHashJoinBase = {

    import org.apache.spark.rdd.RDD
    import org.apache.spark.sql.catalyst.InternalRow
    import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext

    case class NativeShuffledHashJoinExec(
        override val left: SparkPlan,
        override val right: SparkPlan,
        override val leftKeys: Seq[Expression],
        override val rightKeys: Seq[Expression],
        override val joinType: JoinType,
        buildSide: BuildSide,
        skewJoin: Boolean)
        extends NativeShuffledHashJoinBase(left, right, leftKeys, rightKeys, joinType, buildSide)
        with org.apache.spark.sql.execution.joins.ShuffledJoin {

      override def condition: Option[Expression] = None

      override def isSkewJoin: Boolean = false

      override def supportCodegen: Boolean = false

      override def rewriteKeyExprToLong(exprs: Seq[Expression]): Seq[Expression] =
        HashJoin.rewriteKeyExpr(exprs)

      override def inputRDDs(): Seq[RDD[InternalRow]] = {
        throw new NotImplementedError("NativeShuffledHash dose not support codegen")
      }

      override protected def doProduce(ctx: CodegenContext): String = {
        throw new NotImplementedError("NativeShuffledHash dose not support codegen")
      }

      override protected def withNewChildrenInternal(
          newLeft: SparkPlan,
          newRight: SparkPlan): SparkPlan =
        copy(left = newLeft, right = newRight)

      override def nodeName: String =
        "NativeShuffledHashJoinExec" + (if (skewJoin) "(skew=true)" else "")
    }
    NativeShuffledHashJoinExec(left, right, leftKeys, rightKeys, joinType, buildSide, isSkewJoin)
  }

  @sparkver("3.1")
  def provide(
      left: SparkPlan,
      right: SparkPlan,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      buildSide: BuildSide,
      isSkewJoin: Boolean): NativeShuffledHashJoinBase = {

    import org.apache.spark.sql.catalyst.expressions.SortOrder
    import org.apache.spark.sql.execution.auron.plan.BuildLeft
    import org.apache.spark.sql.execution.auron.plan.BuildRight
    import org.apache.spark.sql.execution.joins.ShuffledHashJoinExec

    case class NativeShuffledHashJoinExec(
        override val left: SparkPlan,
        override val right: SparkPlan,
        leftKeys: Seq[Expression],
        rightKeys: Seq[Expression],
        joinType: JoinType,
        buildSide: BuildSide)
        extends NativeShuffledHashJoinBase(left, right, leftKeys, rightKeys, joinType, buildSide)
        with org.apache.spark.sql.execution.joins.ShuffledJoin {

      override def condition: Option[Expression] = None

      override def rewriteKeyExprToLong(exprs: Seq[Expression]): Seq[Expression] =
        HashJoin.rewriteKeyExpr(exprs)

      override def outputOrdering: Seq[SortOrder] = {
        val sparkBuildSide = buildSide match {
          case BuildLeft => org.apache.spark.sql.catalyst.optimizer.BuildLeft
          case BuildRight => org.apache.spark.sql.catalyst.optimizer.BuildRight
        }
        val shj =
          ShuffledHashJoinExec(leftKeys, rightKeys, joinType, sparkBuildSide, None, left, right)
        shj.outputOrdering
      }

      override def withNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
        copy(left = newChildren(0), right = newChildren(1))

      override def nodeName: String = "NativeShuffledHashJoinExec"
    }
    NativeShuffledHashJoinExec(left, right, leftKeys, rightKeys, joinType, buildSide)
  }

  @sparkver("3.0")
  def provide(
      left: SparkPlan,
      right: SparkPlan,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      buildSide: BuildSide,
      isSkewJoin: Boolean): NativeShuffledHashJoinBase = {

    import org.apache.spark.sql.catalyst.expressions.Attribute
    import org.apache.spark.sql.execution.auron.plan.BuildLeft
    import org.apache.spark.sql.execution.auron.plan.BuildRight
    import org.apache.spark.sql.execution.joins.ShuffledHashJoinExec

    case class NativeShuffledHashJoinExec(
        override val left: SparkPlan,
        override val right: SparkPlan,
        leftKeys: Seq[Expression],
        rightKeys: Seq[Expression],
        joinType: JoinType,
        buildSide: BuildSide)
        extends NativeShuffledHashJoinBase(
          left,
          right,
          leftKeys,
          rightKeys,
          joinType,
          buildSide) {

      private def shj: ShuffledHashJoinExec = {
        val sparkBuildSide = buildSide match {
          case BuildLeft => org.apache.spark.sql.execution.joins.BuildLeft
          case BuildRight => org.apache.spark.sql.execution.joins.BuildRight
        }
        ShuffledHashJoinExec(leftKeys, rightKeys, joinType, sparkBuildSide, None, left, right)
      }

      override def output: Seq[Attribute] = shj.output

      override def rewriteKeyExprToLong(exprs: Seq[Expression]): Seq[Expression] =
        HashJoin.rewriteKeyExpr(exprs)

      override val (outputPartitioning, outputOrdering) =
        (shj.outputPartitioning, shj.outputOrdering)

      override def withNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
        copy(left = newChildren(0), right = newChildren(1))

      override def nodeName: String = "NativeShuffledHashJoinExec"
    }
    NativeShuffledHashJoinExec(left, right, leftKeys, rightKeys, joinType, buildSide)
  }
}
