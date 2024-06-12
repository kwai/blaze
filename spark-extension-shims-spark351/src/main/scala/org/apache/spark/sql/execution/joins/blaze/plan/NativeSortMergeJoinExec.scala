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
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.blaze.plan.NativeSortMergeJoinBase
import org.apache.spark.sql.execution.joins.ShuffledJoin

case class NativeSortMergeJoinExec(
    override val left: SparkPlan,
    override val right: SparkPlan,
    override val leftKeys: Seq[Expression],
    override val rightKeys: Seq[Expression],
    override val joinType: JoinType,
    override val condition: Option[Expression])
    extends NativeSortMergeJoinBase(left, right, leftKeys, rightKeys, joinType, condition)
    with ShuffledJoin {

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
}
