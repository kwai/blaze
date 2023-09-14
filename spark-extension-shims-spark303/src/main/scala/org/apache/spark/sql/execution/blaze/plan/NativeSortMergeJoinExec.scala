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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.SortMergeJoinExec

case class NativeSortMergeJoinExec(
    override val left: SparkPlan,
    override val right: SparkPlan,
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression])
    extends NativeSortMergeJoinBase(left, right, leftKeys, rightKeys, joinType, condition) {

  override val (output, outputPartitioning, outputOrdering) = {
    val smj =
      SortMergeJoinExec(leftKeys, rightKeys, joinType, condition, left, right, isSkewJoin = false)
    (smj.output, smj.outputPartitioning, smj.outputOrdering)
  }

  override def withNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
    copy(left = newChildren(0), right = newChildren(1))
}
