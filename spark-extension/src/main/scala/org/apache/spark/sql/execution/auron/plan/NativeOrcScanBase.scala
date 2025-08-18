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

import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.spark.sql.auron.MetricNode
import org.apache.spark.sql.auron.NativeRDD
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.FilePartition
import org.auron.{protobuf => pb}

abstract class NativeOrcScanBase(basedFileScan: FileSourceScanExec)
    extends NativeFileSourceScanBase(basedFileScan) {

  override def doExecuteNative(): NativeRDD = {
    val partitions = inputFileScanRDD.filePartitions.toArray
    val nativeMetrics = MetricNode(
      metrics,
      Nil,
      Some({
        case ("bytes_scanned", v) =>
          val inputMetric = TaskContext.get.taskMetrics().inputMetrics
          inputMetric.incBytesRead(v)
        case ("output_rows", v) =>
          val inputMetric = TaskContext.get.taskMetrics().inputMetrics
          inputMetric.incRecordsRead(v)
        case _ =>
      }))
    val nativePruningPredicateFilters = this.nativePruningPredicateFilters
    val nativeFileSchema = this.nativeFileSchema
    val nativeFileGroups = this.nativeFileGroups
    val nativePartitionSchema = this.nativePartitionSchema
    val projection = schema.map(field => basedFileScan.relation.schema.fieldIndex(field.name))
    val broadcastedHadoopConf = this.broadcastedHadoopConf
    val numPartitions = partitions.length

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      partitions.asInstanceOf[Array[Partition]],
      None,
      Nil,
      rddShuffleReadFull = true,
      (partition, _) => {
        val resourceId = s"NativeOrcScanExec:${UUID.randomUUID().toString}"
        putJniBridgeResource(resourceId, broadcastedHadoopConf)

        val nativeFileGroup = nativeFileGroups(partition.asInstanceOf[FilePartition])
        val nativeFileScanExecConf = pb.FileScanExecConf
          .newBuilder()
          .setNumPartitions(numPartitions)
          .setPartitionIndex(partition.index)
          .setStatistics(pb.Statistics.getDefaultInstance)
          .setSchema(nativeFileSchema)
          .setFileGroup(nativeFileGroup)
          .addAllProjection(projection.map(Integer.valueOf).asJava)
          .setPartitionSchema(nativePartitionSchema)
          .build()

        val nativeOrcScanExecBuilder = pb.OrcScanExecNode
          .newBuilder()
          .setBaseConf(nativeFileScanExecConf)
          .setFsResourceId(resourceId)
          .addAllPruningPredicates(nativePruningPredicateFilters.asJava)

        pb.PhysicalPlanNode
          .newBuilder()
          .setOrcScan(nativeOrcScanExecBuilder.build())
          .build()
      },
      friendlyName = "NativeRDD.OrcScan")
  }

  override val nodeName: String =
    s"NativeOrcScan ${basedFileScan.tableIdentifier.map(_.unquotedString).getOrElse("")}"
}
