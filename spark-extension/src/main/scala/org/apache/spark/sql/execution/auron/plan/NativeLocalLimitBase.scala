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

import scala.collection.immutable.SortedMap

import org.apache.spark.sql.auron.MetricNode
import org.apache.spark.sql.auron.NativeRDD
import org.apache.spark.sql.auron.NativeHelper
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.OneToOneDependency
import org.apache.auron.protobuf.LimitExecNode
import org.apache.auron.protobuf.PhysicalPlanNode
import org.apache.spark.sql.auron.NativeSupports

abstract class NativeLocalLimitBase(limit: Long, override val child: SparkPlan)
    extends UnaryExecNode
    with NativeSupports {

  override lazy val metrics: Map[String, SQLMetric] = SortedMap[String, SQLMetric]() ++ Map(
    NativeHelper
      .getDefaultNativeMetrics(sparkContext)
      .filterKeys(Set("stage_id", "output_rows"))
      .toSeq: _*)

  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def doExecuteNative(): NativeRDD = {
    val inputRDD = NativeHelper.executeNative(child)
    val nativeMetrics = MetricNode(metrics, inputRDD.metrics :: Nil)

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      rddPartitions = inputRDD.partitions,
      rddPartitioner = inputRDD.partitioner,
      rddDependencies = new OneToOneDependency(inputRDD) :: Nil,
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
}
