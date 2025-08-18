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
import org.apache.spark.sql.execution.auron.plan.NativeSortMergeJoinBase
import org.auron.sparkver

case object NativeSortMergeJoinExecProvider {

  @sparkver("3.2 / 3.3 / 3.4 / 3.5")
  def provide(
      left: SparkPlan,
      right: SparkPlan,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      skewJoin: Boolean): NativeSortMergeJoinBase = {

    import org.apache.spark.rdd.RDD
    import org.apache.spark.sql.catalyst.InternalRow
    import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext

    case class NativeSortMergeJoinExec(
        override val left: SparkPlan,
        override val right: SparkPlan,
        override val leftKeys: Seq[Expression],
        override val rightKeys: Seq[Expression],
        override val joinType: JoinType,
        skewJoin: Boolean)
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

      override def nodeName: String =
        "NativeSortMergeJoinExec" + (if (skewJoin) "(skew=true)" else "")
    }
    NativeSortMergeJoinExec(left, right, leftKeys, rightKeys, joinType, skewJoin)
  }

  @sparkver("3.0 / 3.1")
  def provide(
      left: SparkPlan,
      right: SparkPlan,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      skewJoin: Boolean): NativeSortMergeJoinBase = {

    import org.apache.spark.sql.catalyst.expressions.Attribute
    import org.apache.spark.sql.execution.joins.SortMergeJoinExec

    case class NativeSortMergeJoinExec(
        override val left: SparkPlan,
        override val right: SparkPlan,
        leftKeys: Seq[Expression],
        rightKeys: Seq[Expression],
        joinType: JoinType,
        skewJoin: Boolean)
        extends NativeSortMergeJoinBase(left, right, leftKeys, rightKeys, joinType) {

      private def smj: SortMergeJoinExec =
        SortMergeJoinExec(leftKeys, rightKeys, joinType, None, left, right, isSkewJoin = false)

      override def output: Seq[Attribute] = smj.output

      override val (outputPartitioning, outputOrdering) =
        (smj.outputPartitioning, smj.outputOrdering)

      override def withNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
        copy(left = newChildren(0), right = newChildren(1))

      override def nodeName: String =
        "NativeSortMergeJoinExec" + (if (skewJoin) "(skew=true)" else "")
    }
    NativeSortMergeJoinExec(left, right, leftKeys, rightKeys, joinType, skewJoin)
  }
}
