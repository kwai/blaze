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

package org.apache.spark.sql.blaze

import java.util.UUID

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.util2.ArrowWriter
import org.apache.spark.SparkEnv
import org.apache.spark.sql.blaze.execution.ArrowWriterIterator
import org.apache.spark.sql.blaze.execution.Converters
import org.apache.spark.sql.internal.SQLConf
import org.blaze.protobuf.PhysicalPlanNode
import org.blaze.protobuf.ShuffleReaderExecNode

case class ConvertToNativeExec(override val child: SparkPlan)
    extends UnaryExecNode
    with NativeSupports {
  override def nodeName: String = "ConvertToNative"
  override def logicalLink: Option[LogicalPlan] = child.logicalLink
  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = child.outputPartitioning

  override lazy val metrics: Map[String, SQLMetric] =
    NativeSupports.getDefaultNativeMetrics(sparkContext)

  override def doExecute(): RDD[InternalRow] = doExecuteNative()
  override def doExecuteNative(): NativeRDD = {
    val inputRDD = child.execute()
    val timeZoneId = SparkEnv.get.conf.get(SQLConf.SESSION_LOCAL_TIMEZONE)
    val nativeMetrics = MetricNode(metrics, Nil)

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      inputRDD.partitions,
      inputRDD.dependencies,
      (partition, context) => {
        val inputRowIter = inputRDD.compute(partition, context)
        val arrowWriterIterator =
          new ArrowWriterIterator(inputRowIter, schema, timeZoneId, context)

        val resourceId = "ConvertToNativeExec" +
          s":stage=${context.stageId()}" +
          s":partition=${context.partitionId()}" +
          s":taskAttempt=${context.taskAttemptId()}" +
          s":uuid=${UUID.randomUUID().toString}"
        JniBridge.resourcesMap.put(resourceId, arrowWriterIterator)

        PhysicalPlanNode
          .newBuilder()
          .setShuffleReader(
            ShuffleReaderExecNode
              .newBuilder()
              .setSchema(NativeConverters.convertSchema(schema))
              .setNativeShuffleId(resourceId)
              .build())
          .build()
      })
  }
}
