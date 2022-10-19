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

import org.apache.spark.sql.blaze.{MetricNode, NativeRDD, NativeSupports}
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{LocalLimitExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.blaze.protobuf.{LimitExecNode, PhysicalPlanNode}

case class NativeLocalLimitExec(limit: Long, override val child: SparkPlan)
    extends UnaryExecNode
    with NativeSupports {

  override lazy val metrics: Map[String, SQLMetric] =
    NativeSupports.getDefaultNativeMetrics(sparkContext)

  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def doExecuteNative(): NativeRDD = {
    val inputRDD = NativeSupports.executeNative(child)
    val nativeMetrics = MetricNode(metrics, inputRDD.metrics :: Nil)

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      inputRDD.partitions,
      inputRDD.dependencies,
      rddShuffleReadFull = false,
      (partition, taskContext) => {
        val inputPartition = inputRDD.partitions(partition.index)
        val nativeLimitExec = LimitExecNode
          .newBuilder()
          .setInput(inputRDD.nativePlan(inputPartition, taskContext))
          .setLimit(limit)
          .build()
        PhysicalPlanNode.newBuilder().setLimit(nativeLimitExec).build()
      },
      friendlyName = "NativeRDD.LocalLimit")
  }

  override def doCanonicalize(): SparkPlan =
    LocalLimitExec(limit.toInt, child).canonicalized
}
