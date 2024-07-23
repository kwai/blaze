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
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.blaze.plan.NativeSortMergeJoinBase

import com.thoughtworks.enableIf

case object NativeSortMergeJoinExecProvider {

  @enableIf(Seq("spark324", "spark333", "spark351").contains(System.getProperty("blaze.shim")))
  def provide(
      left: SparkPlan,
      right: SparkPlan,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType): NativeSortMergeJoinBase = {

    import org.apache.spark.rdd.RDD
    import org.apache.spark.sql.catalyst.InternalRow
    import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext

    case class NativeSortMergeJoinExec(
        override val left: SparkPlan,
        override val right: SparkPlan,
        override val leftKeys: Seq[Expression],
        override val rightKeys: Seq[Expression],
        override val joinType: JoinType)
        extends NativeSortMergeJoinBase(left, right, leftKeys, rightKeys, joinType)
        with org.apache.spark.sql.execution.joins.ShuffledJoin {

      override def condition: Option[Expression] = None

      override def isSkewJoin: Boolean = false

      override def supportCodegen: Boolean = false

      override def inputRDDs(): Seq[RDD[InternalRow]] = {
        throw new NotImplementedError("NativeSortMergeJoin dose not support codegen")
      }

      override protected def doProduce(ctx: CodegenContext): String = {
        throw new NotImplementedError("NativeSortMergeJoin dose not support codegen")
      }

      override protected def withNewChildrenInternal(
          newLeft: SparkPlan,
          newRight: SparkPlan): SparkPlan =
        copy(left = newLeft, right = newRight)

      override def nodeName: String = "NativeSortMergeJoinExec"
    }
    NativeSortMergeJoinExec(left, right, leftKeys, rightKeys, joinType)
  }

  @enableIf(Seq("spark303").contains(System.getProperty("blaze.shim")))
  def provide(
      left: SparkPlan,
      right: SparkPlan,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType): NativeSortMergeJoinBase = {

    import org.apache.spark.sql.execution.joins.SortMergeJoinExec

    case class NativeSortMergeJoinExec(
        override val left: SparkPlan,
        override val right: SparkPlan,
        leftKeys: Seq[Expression],
        rightKeys: Seq[Expression],
        joinType: JoinType)
        extends NativeSortMergeJoinBase(left, right, leftKeys, rightKeys, joinType) {

      override val (output, outputPartitioning, outputOrdering) = {
        val smj =
          SortMergeJoinExec(leftKeys, rightKeys, joinType, None, left, right, isSkewJoin = false)
        (smj.output, smj.outputPartitioning, smj.outputOrdering)
      }

      override def withNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
        copy(left = newChildren(0), right = newChildren(1))

      override def nodeName: String = "NativeSortMergeJoinExec"
    }
    NativeSortMergeJoinExec(left, right, leftKeys, rightKeys, joinType)
  }
}
