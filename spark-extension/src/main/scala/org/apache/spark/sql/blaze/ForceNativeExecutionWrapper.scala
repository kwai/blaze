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

package org.apache.spark.sql.blaze

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode

case class ForceNativeExecutionWrapper(override val child: SparkPlan)
    extends UnaryExecNode
    with NativeSupports {
  override val output: Seq[Attribute] = child.output
  override val outputPartitioning: Partitioning = child.outputPartitioning
  override val outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def doExecuteNative(): NativeRDD = Shims.get.sparkPlanShims.executeNative(child)

  override val nodeName: String = "InputAdapter"
}
