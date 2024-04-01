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

import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.execution.AliasAwareOutputOrdering
import org.apache.spark.sql.execution.AliasAwareOutputPartitioning
import org.apache.spark.sql.execution.SparkPlan

case class NativeRenameColumnsExec(override val child: SparkPlan, renamedColumnNames: Seq[String])
    extends NativeRenameColumnsBase(child, renamedColumnNames)
    with AliasAwareOutputPartitioning
    with AliasAwareOutputOrdering {

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)

  override protected def outputExpressions: Seq[NamedExpression] = output

  override protected def orderingExpressions: Seq[SortOrder] = child.outputOrdering
}
