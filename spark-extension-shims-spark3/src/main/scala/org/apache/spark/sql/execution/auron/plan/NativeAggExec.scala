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
package org.apache.spark.sql.execution.auron.plan

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.ExprId
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.Final
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.auron.plan.NativeAggBase.AggExecMode
import org.apache.spark.sql.types.BinaryType

import org.apache.auron.sparkver

case class NativeAggExec(
    execMode: AggExecMode,
    theRequiredChildDistributionExpressions: Option[Seq[Expression]],
    override val groupingExpressions: Seq[NamedExpression],
    override val aggregateExpressions: Seq[AggregateExpression],
    override val aggregateAttributes: Seq[Attribute],
    theInitialInputBufferOffset: Int,
    override val child: SparkPlan)
    extends NativeAggBase(
      execMode,
      theRequiredChildDistributionExpressions,
      groupingExpressions,
      aggregateExpressions,
      aggregateAttributes,
      theInitialInputBufferOffset,
      child)
    with BaseAggregateExec {

  @sparkver("3.1 / 3.2 / 3.3 / 3.4 / 3.5")
  override val requiredChildDistributionExpressions: Option[Seq[Expression]] =
    theRequiredChildDistributionExpressions

  @sparkver("3.3 / 3.4 / 3.5")
  override val initialInputBufferOffset: Int = theInitialInputBufferOffset

  override def output: Seq[Attribute] =
    if (aggregateExpressions.map(_.mode).contains(Final)) {
      groupingExpressions.map(_.toAttribute) ++ aggregateAttributes
    } else {
      groupingExpressions.map(_.toAttribute) :+
        AttributeReference(NativeAggBase.AGG_BUF_COLUMN_NAME, BinaryType, nullable = false)(
          ExprId.apply(NativeAggBase.AGG_BUF_COLUMN_EXPR_ID))
    }

  @sparkver("3.2 / 3.3 / 3.4 / 3.5")
  override def isStreaming: Boolean = false

  @sparkver("3.2 / 3.3 / 3.4 / 3.5")
  override def numShufflePartitions: Option[Int] = None

  override def resultExpressions: Seq[NamedExpression] = output

  @sparkver("3.2 / 3.3 / 3.4 / 3.5")
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)

  @sparkver("3.0 / 3.1")
  override def withNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
    copy(child = newChildren.head)
}
