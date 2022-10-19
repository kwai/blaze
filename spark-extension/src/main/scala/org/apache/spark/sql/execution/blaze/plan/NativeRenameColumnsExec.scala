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
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.blaze.plan.NativeRenameColumnsExec.buildRenameColumnsExec
import org.apache.spark.sql.execution.metric.SQLMetric
import org.blaze.protobuf.{PhysicalPlanNode, RenameColumnsExecNode}

import scala.collection.JavaConverters._

case class NativeRenameColumnsExec(override val child: SparkPlan, renamedColumnNames: Seq[String])
    extends UnaryExecNode
    with NativeSupports {

  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override lazy val metrics: Map[String, SQLMetric] =
    NativeSupports.getDefaultNativeMetrics(sparkContext)

  override def doExecuteNative(): NativeRDD = {
    val inputRDD = NativeSupports.executeNative(child)
    val nativeMetrics = MetricNode(metrics, inputRDD.metrics :: Nil)

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      inputRDD.partitions,
      inputRDD.dependencies,
      inputRDD.shuffleReadFull,
      (partition, taskContext) => {
        val inputPlan = inputRDD.nativePlan(inputRDD.partitions(partition.index), taskContext)
        buildRenameColumnsExec(inputPlan, renamedColumnNames)
      },
      friendlyName = "NativeRDD.RenameColumns")
  }

  override def doCanonicalize(): SparkPlan = child.canonicalized
}

object NativeRenameColumnsExec {
  def buildRenameColumnsExec(
      input: PhysicalPlanNode,
      newColumnNames: Seq[String]): PhysicalPlanNode = {
    PhysicalPlanNode
      .newBuilder()
      .setRenameColumns(
        RenameColumnsExecNode
          .newBuilder()
          .setInput(input)
          .addAllRenamedColumnNames(newColumnNames.asJava))
      .build()
  }
}
