/*
 * Copyright 2022 The Auron Authors
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
package org.apache.spark.sql.execution.auron.plan

import scala.collection.immutable.SortedMap
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters.asJavaIterableConverter

import org.apache.spark.rdd.PartitionerAwareUnionRDD
import org.apache.spark.rdd.PartitionerAwareUnionRDDPartition
import org.apache.spark.rdd.UnionPartition
import org.apache.spark.rdd.UnionRDD
import org.apache.spark.sql.auron.MetricNode
import org.apache.spark.sql.auron.NativeHelper
import org.apache.spark.sql.auron.NativeRDD
import org.apache.spark.sql.auron.NativeSupports
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.auron.protobuf.EmptyPartitionsExecNode
import org.auron.protobuf.PhysicalPlanNode
import org.auron.protobuf.Schema
import org.auron.protobuf.UnionExecNode
import org.auron.protobuf.UnionInput

abstract class NativeUnionBase(
    override val children: Seq[SparkPlan],
    override val output: Seq[Attribute])
    extends SparkPlan
    with NativeSupports {

  override lazy val metrics: Map[String, SQLMetric] = SortedMap[String, SQLMetric]() ++ Map(
    NativeHelper
      .getDefaultNativeMetrics(sparkContext)
      .filterKeys(Set("stage_id", "output_rows"))
      .toSeq: _*)

  override def doExecuteNative(): NativeRDD = {
    val rdds = children.map(c => NativeHelper.executeNative(c))
    val nativeMetrics = MetricNode(metrics, rdds.filter(_.partitions.nonEmpty).map(_.metrics))
    val unionRDD = sparkContext.union(rdds)
    val unionedPartitions = unionRDD.partitions
    val unionedPartitioner = unionRDD.partitioner
    val dependencies = unionRDD.dependencies

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      unionedPartitions,
      unionedPartitioner,
      dependencies,
      rdds.forall(_.isShuffleReadFull),
      (partition, taskContext) => {
        val unionInputs = ArrayBuffer[(PhysicalPlanNode, Int)]()
        partition match {
          case p: UnionPartition[_] =>
            val rdds = unionRDD.asInstanceOf[UnionRDD[_]].rdds
            val nativeRDD = rdds(p.parentRddIndex).asInstanceOf[NativeRDD]
            val input = nativeRDD.nativePlan(p.parentPartition, taskContext)
            for (childIndex <- rdds.indices) {
              if (childIndex == p.parentRddIndex) {
                unionInputs.append((input, p.parentPartition.index))
              } else {
                unionInputs.append((nativeEmptyPartitionExec(1), 0))
              }
            }
          case p: PartitionerAwareUnionRDDPartition =>
            val rdds = unionRDD.asInstanceOf[PartitionerAwareUnionRDD[_]].rdds
            for ((rdd, partition) <- rdds.zip(p.parents)) {
              val nativeRDD = rdd.asInstanceOf[NativeRDD]
              unionInputs.append((nativeRDD.nativePlan(partition, taskContext), partition.index))
            }
        }

        val union = UnionExecNode
          .newBuilder()
          .addAllInput(unionInputs.map { case (input, partition) =>
            UnionInput
              .newBuilder()
              .setInput(input)
              .setPartition(partition)
              .build()
          }.asJava)
          .setNumPartitions(unionedPartitions.length)
          .setCurPartition(partition.index)
          .setSchema(nativeSchema)
        PhysicalPlanNode.newBuilder().setUnion(union).build()
      },
      friendlyName = "NativeRDD.Union")
  }

  private def nativeEmptyPartitionExec(numPartitions: Int) =
    PhysicalPlanNode
      .newBuilder()
      .setEmptyPartitions(
        EmptyPartitionsExecNode
          .newBuilder()
          .setSchema(nativeSchema)
          .setNumPartitions(numPartitions)
          .build())
      .build()

  val nativeSchema: Schema = Util.getNativeSchema(output)
}
