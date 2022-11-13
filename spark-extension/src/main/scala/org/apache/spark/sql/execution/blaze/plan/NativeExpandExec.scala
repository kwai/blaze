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
import org.apache.spark.sql.blaze.NativeConverters
import org.apache.spark.sql.blaze.NativeRDD
import org.apache.spark.sql.blaze.NativeSupports
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.OneToOneDependency
import org.apache.spark.sql.catalyst.plans.physical.UnknownPartitioning
import org.apache.spark.sql.execution.ExpandExec
import org.blaze.protobuf.ExpandExecNode
import org.blaze.protobuf.ExpandProjection
import org.blaze.protobuf.PhysicalPlanNode

case class NativeExpandExec(
    projections: Seq[Seq[Expression]],
    override val output: Seq[Attribute],
    override val child: SparkPlan)
    extends UnaryExecNode
    with NativeSupports {

  override lazy val metrics: Map[String, SQLMetric] = Map(
    NativeSupports
      .getDefaultNativeMetrics(sparkContext)
      .filterKeys(Set("output_rows", "elapsed_compute"))
      .toSeq: _*)

  override val outputPartitioning: Partitioning = UnknownPartitioning(0)
  override val outputOrdering: Seq[SortOrder] = Nil

  private val nativeSchema = Util.getNativeSchema(output)
  private val nativeProjections = projections.map { projection =>
    projection.map(expr => NativeConverters.convertExpr(expr))
  }

  override def doExecuteNative(): NativeRDD = {
    val inputRDD = NativeSupports.executeNative(child)
    val nativeMetrics = MetricNode(metrics, inputRDD.metrics :: Nil)

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      rddPartitions = inputRDD.partitions,
      rddDependencies = new OneToOneDependency(inputRDD) :: Nil,
      inputRDD.shuffleReadFull,
      (partition, taskContext) => {
        val inputPartition = inputRDD.partitions(partition.index)
        val nativeExpandExec = ExpandExecNode
          .newBuilder()
          .setInput(inputRDD.nativePlan(inputPartition, taskContext))
          .setSchema(nativeSchema)
          .addAllProjections(nativeProjections.map { projection =>
            ExpandProjection
              .newBuilder()
              .addAllExpr(projection.asJava)
              .build()
          }.asJava)
          .build()
        PhysicalPlanNode.newBuilder().setExpand(nativeExpandExec).build()
      },
      friendlyName = "NativeRDD.Expand")
  }

  override def doCanonicalize(): SparkPlan =
    ExpandExec(projections, output, child).canonicalized

  override def withNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
    copy(child = newChildren.head)
}
