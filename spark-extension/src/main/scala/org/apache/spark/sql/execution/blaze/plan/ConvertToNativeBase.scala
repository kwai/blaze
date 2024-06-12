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

import java.util.UUID

import scala.collection.immutable.SortedMap

import org.apache.spark.InterruptibleIterator
import org.apache.spark.sql.blaze.JniBridge
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.blaze.MetricNode
import org.apache.spark.sql.blaze.NativeConverters
import org.apache.spark.sql.blaze.NativeRDD
import org.apache.spark.sql.blaze.NativeHelper
import org.apache.spark.sql.execution.blaze.arrowio.ArrowFFIExportIterator
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.OneToOneDependency
import org.blaze.protobuf.FFIReaderExecNode
import org.blaze.protobuf.PhysicalPlanNode
import org.blaze.protobuf.Schema
import org.apache.spark.sql.blaze.NativeSupports
import org.apache.spark.sql.blaze.Shims
import org.apache.spark.sql.catalyst.expressions.SortOrder

abstract class ConvertToNativeBase(override val child: SparkPlan)
    extends UnaryExecNode
    with NativeSupports {
  override val nodeName: String = "ConvertToNative"
  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override lazy val metrics: Map[String, SQLMetric] = SortedMap[String, SQLMetric]() ++ Map(
    NativeHelper
      .getDefaultNativeMetrics(sparkContext)
      .filterKeys(Set("stage_id", "output_rows", "elapsed_compute"))
      .toSeq :+
      ("size", SQLMetrics.createSizeMetric(sparkContext, "Native.batch_bytes_size")): _*)

  val renamedSchema: StructType = Util.getSchema(child.output)
  val nativeSchema: Schema = NativeConverters.convertSchema(renamedSchema)

  override def doExecuteNative(): NativeRDD = {
    val inputRDD = child.execute()
    val numInputPartitions = inputRDD.getNumPartitions
    val nativeMetrics = MetricNode(metrics, Nil)

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      rddPartitions = inputRDD.partitions,
      rddDependencies = new OneToOneDependency(inputRDD) :: Nil,
      Shims.get.getRDDShuffleReadFull(inputRDD),
      (partition, context) => {
        val inputRowIter = inputRDD.compute(partition, context)
        val resourceId = s"ConvertToNativeExec:${UUID.randomUUID().toString}"
        JniBridge.resourcesMap.put(
          resourceId,
          () => {
            val exportIter = new ArrowFFIExportIterator(inputRowIter, renamedSchema)
            new InterruptibleIterator(context, exportIter)
          })

        PhysicalPlanNode
          .newBuilder()
          .setFfiReader(
            FFIReaderExecNode
              .newBuilder()
              .setSchema(nativeSchema)
              .setNumPartitions(numInputPartitions)
              .setExportIterProviderResourceId(resourceId)
              .build())
          .build()
      },
      friendlyName = "NativeRDD.ConvertToNative")
  }

  // ignore this wrapper
  override protected def doCanonicalize(): SparkPlan = child.canonicalized
}
