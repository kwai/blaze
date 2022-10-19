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

import org.apache.spark.{InterruptibleIterator, SparkEnv}
import org.apache.spark.sql.blaze._
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.blaze.arrowio.ArrowFFIExportIterator
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}
import org.blaze.protobuf.{FFIReaderExecNode, PhysicalPlanNode, Schema}

import java.util.UUID

case class ConvertToNativeExec(override val child: SparkPlan)
    extends UnaryExecNode
    with NativeSupports {
  override def nodeName: String = "ConvertToNative"

  def logicalLink: Option[LogicalPlan] = child.logicalLink

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override lazy val metrics: Map[String, SQLMetric] =
    NativeSupports.getDefaultNativeMetrics(sparkContext) ++ Map(
      "size" -> SQLMetrics.createSizeMetric(sparkContext, "Native.batch_bytes_size"))

  val renamedSchema: StructType =
    StructType(
      output.map(a => StructField(s"#${a.exprId.id}", a.dataType, a.nullable, a.metadata)))
  val nativeSchema: Schema = NativeConverters.convertSchema(renamedSchema)

  override def doExecuteNative(): NativeRDD = {
    val inputRDD = child.execute()
    val numInputPartitions = inputRDD.getNumPartitions
    val timeZoneId = SparkEnv.get.conf.get(SQLConf.SESSION_LOCAL_TIMEZONE)
    val nativeMetrics = MetricNode(metrics, Nil)

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      inputRDD.partitions,
      inputRDD.dependencies,
      inputRDD.shuffleReadFull,
      (partition, context) => {
        val inputRowIter = inputRDD.compute(partition, context)
        val resourceId = s"ConvertToNativeExec:${UUID.randomUUID().toString}"
        JniBridge.resourcesMap.put(
          resourceId,
          () => {
            val exportIter =
              new ArrowFFIExportIterator(inputRowIter, renamedSchema, timeZoneId, context)
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

  override def doCanonicalize(): SparkPlan = child.canonicalized
}
