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

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.blaze.plan.NativeAggBase.AggExecMode

case class NativeAggExec(
    execMode: AggExecMode,
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    override val child: SparkPlan)
    extends NativeAggBase(
      execMode,
      requiredChildDistributionExpressions,
      groupingExpressions,
      aggregateExpressions,
      aggregateAttributes,
      initialInputBufferOffset,
      child) {

  override def withNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
    copy(child = newChildren.head)
}
