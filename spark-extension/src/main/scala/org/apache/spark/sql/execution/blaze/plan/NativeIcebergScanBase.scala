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

import org.apache.hadoop.fs.FileSystem
import org.apache.iceberg.FileScanTask
import org.apache.iceberg.spark.source.{IcebergBatchQueryScan, IcebergInputPartition}
import org.apache.spark.sql.blaze._
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceRDD, DataSourceRDDPartition}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.{Partition, TaskContext}
import org.blaze.{protobuf => pb}

import java.net.URI
import java.security.PrivilegedExceptionAction
import java.util.{Collections, UUID}
import scala.collection.JavaConverters._
import scala.collection.mutable

abstract class NativeIcebergScanBase(baseDataSourceScan: BatchScanExec)
    extends LeafExecNode
    with NativeSupports {

  override lazy val metrics: Map[String, SQLMetric] = mutable
    .LinkedHashMap(
      NativeHelper
        .getDefaultNativeMetrics(sparkContext)
        .filterKeys(Set("output_rows", "elapsed_compute"))
        .toSeq :+
        ("predicate_evaluation_errors", SQLMetrics
          .createMetric(sparkContext, "Native.predicate_evaluation_errors")) :+
        ("row_groups_pruned", SQLMetrics
          .createMetric(sparkContext, "Native.row_groups_pruned")) :+
        ("bytes_scanned", SQLMetrics.createSizeMetric(sparkContext, "Native.bytes_scanned")) :+
        ("io_time", SQLMetrics.createNanoTimingMetric(sparkContext, "Native.io_time")) :+
        ("io_time_getfs", SQLMetrics
          .createNanoTimingMetric(sparkContext, "Native.io_time_getfs")): _*)
    .toMap

  val sparkSession = Shims.get.getSqlContext(baseDataSourceScan).sparkSession
  val scan = new IcebergBatchQueryScan(baseDataSourceScan.scan)

  override val output: Seq[Attribute] = baseDataSourceScan.output
  override val outputPartitioning: Partitioning = baseDataSourceScan.outputPartitioning

  private def nativePruningPredicateFilters = Seq(
    NativeConverters.convertScanPruningExpr(scan.dataFilter(sparkSession)))

  private def nativeFileSchema = NativeConverters.convertSchema(scan.tableSchema())
  private def nativePartitionSchema = NativeConverters.convertSchema(StructType.apply(Seq.empty))

  private def nativeFileGroups = (partition: DataSourceRDDPartition) => {
    // list input file statuses
    val nativePartitionedFile = (file: FileScanTask) => {

      pb.PartitionedFile
        .newBuilder()
        .setPath(file.file().path().toString)
        .setSize(file.length())
        .addAllPartitionValues(Collections.emptyList())
        .setLastModifiedNs(0)
        .setRange(
          pb.FileRange
            .newBuilder()
            .setStart(file.start())
            .setEnd(file.start() + file.length())
            .build())
        .build()
    }

    val tasks: Seq[FileScanTask] = partition.inputPartitions
      .flatMap(x => IcebergInputPartition.tasks(x).tasks().asScala)
      .asInstanceOf[Seq[FileScanTask]]

    pb.FileGroup
      .newBuilder()
      .addAllFiles(tasks.map(nativePartitionedFile).toList.asJava)
      .build()
  }

  // check whether native converting is supported
  nativePruningPredicateFilters
  nativeFileSchema
  nativePartitionSchema
  nativeFileGroups

  override def doExecuteNative(): NativeRDD = {
    val partitions: Array[Partition] = baseDataSourceScan.inputRDD match {
      case rdd: DataSourceRDD => rdd.partitions
      case _ => Array()
    }
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

    val projection = schema.map(field => scan.projection().fieldIndex(field.name))

    val hadoopConf = scan.getConf();
    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    val numPartitions = partitions.length

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      partitions,
      Nil,
      rddShuffleReadFull = true,
      (partition, context) => {
        val resourceId = s"NativeParquetScanExec:${UUID.randomUUID().toString}"
        val sharedConf = broadcastedHadoopConf.value.value
        JniBridge.resourcesMap.put(
          resourceId,
          (location: String) => {
            val getfsTimeMetric = metrics("io_time_getfs")
            val currentTimeMillis = System.currentTimeMillis()
            val fs = NativeHelper.currentUser.doAs(new PrivilegedExceptionAction[FileSystem] {
              override def run(): FileSystem = {
                FileSystem.get(new URI(location), sharedConf)
              }
            })
            getfsTimeMetric.add((System.currentTimeMillis() - currentTimeMillis) * 1000000)
            fs
          })

        val nativeFileGroup = nativeFileGroups(partition.asInstanceOf[DataSourceRDDPartition])

        val nativeParquetScanConf = pb.FileScanExecConf
          .newBuilder()
          .setNumPartitions(numPartitions)
          .setPartitionIndex(partition.index)
          .setStatistics(pb.Statistics.getDefaultInstance)
          .setSchema(nativeFileSchema)
          .setFileGroup(nativeFileGroup)
          .addAllProjection(projection.map(Integer.valueOf).asJava)
          .setPartitionSchema(nativePartitionSchema)
          .build()

        val nativeParquetScanExecBuilder = pb.ParquetScanExecNode
          .newBuilder()
          .setBaseConf(nativeParquetScanConf)
          .setFsResourceId(resourceId)
          .addAllPruningPredicates(nativePruningPredicateFilters.asJava)

        pb.PhysicalPlanNode
          .newBuilder()
          .setParquetScan(nativeParquetScanExecBuilder.build())
          .build()
      },
      friendlyName = "NativeRDD.ParquetScan")
  }

  override val nodeName: String = s"NativeParquetScan ${scan.tableIdent()}"

  override protected def doCanonicalize(): SparkPlan = baseDataSourceScan.canonicalized
}
