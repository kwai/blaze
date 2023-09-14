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

import scala.collection.JavaConverters._

import org.apache.spark.sql.blaze.MetricNode
import org.apache.spark.sql.blaze.NativeRDD
import org.apache.spark.sql.blaze.NativeHelper
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.blaze.plan.NativeRenameColumnsBase.buildRenameColumnsExec
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.OneToOneDependency
import org.blaze.protobuf.PhysicalPlanNode
import org.blaze.protobuf.RenameColumnsExecNode

import org.apache.spark.sql.blaze.NativeSupports

abstract class NativeRenameColumnsBase(
    override val child: SparkPlan,
    renamedColumnNames: Seq[String])
    extends UnaryExecNode
    with NativeSupports {

  override def output: Seq[Attribute] =
    child.output
      .zip(renamedColumnNames)
      .map { case (attr, newName) =>
        attr.withName(newName)
      }
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override lazy val metrics: Map[String, SQLMetric] = Map()

  override def doExecuteNative(): NativeRDD = {
    val inputRDD = NativeHelper.executeNative(child)
    val nativeMetrics = MetricNode(metrics, inputRDD.metrics :: Nil)

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      rddPartitions = inputRDD.partitions,
      rddDependencies = new OneToOneDependency(inputRDD) :: Nil,
      inputRDD.isShuffleReadFull,
      (partition, taskContext) => {
        val inputPlan = inputRDD.nativePlan(inputRDD.partitions(partition.index), taskContext)
        buildRenameColumnsExec(inputPlan, renamedColumnNames)
      },
      friendlyName = "NativeRDD.RenameColumns")
  }

  override def nodeName: String = "InputAdapter"

  // ignore this wrapper
  override protected def doCanonicalize(): SparkPlan = child.canonicalized
}

object NativeRenameColumnsBase {
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
