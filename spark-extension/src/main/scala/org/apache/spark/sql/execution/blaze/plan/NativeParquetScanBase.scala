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

import java.net.URI
import java.security.PrivilegedExceptionAction
import java.util.UUID

import scala.collection.JavaConverters._
import scala.collection.immutable.SortedMap

import org.apache.commons.lang3.reflect.MethodUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.blaze.{protobuf => pb}
import org.apache.spark.rdd.MapPartitionsRDD
import org.apache.spark.sql.blaze.JniBridge
import org.apache.spark.sql.blaze.MetricNode
import org.apache.spark.sql.blaze.NativeConverters
import org.apache.spark.sql.blaze.NativeHelper
import org.apache.spark.sql.blaze.NativeRDD
import org.apache.spark.sql.blaze.NativeSupports
import org.apache.spark.sql.blaze.Shims
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.datasources.FileScanRDD
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.NullType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

abstract class NativeParquetScanBase(basedFileScan: FileSourceScanExec)
    extends LeafExecNode
    with NativeSupports {

  override lazy val metrics: Map[String, SQLMetric] = SortedMap[String, SQLMetric]() ++ Map(
    NativeHelper
      .getDefaultNativeMetrics(sparkContext)
      .filterKeys(Set("stage_id", "output_rows", "elapsed_compute"))
      .toSeq :+
      ("predicate_evaluation_errors", SQLMetrics
        .createMetric(sparkContext, "Native.predicate_evaluation_errors")) :+
      ("row_groups_pruned", SQLMetrics
        .createMetric(sparkContext, "Native.row_groups_pruned")) :+
      ("bytes_scanned", SQLMetrics.createSizeMetric(sparkContext, "Native.bytes_scanned")) :+
      ("io_time", SQLMetrics.createNanoTimingMetric(sparkContext, "Native.io_time")) :+
      ("io_time_getfs", SQLMetrics
        .createNanoTimingMetric(sparkContext, "Native.io_time_getfs")): _*)

  override val output: Seq[Attribute] = basedFileScan.output
  override val outputPartitioning: Partitioning = basedFileScan.outputPartitioning

  private val inputFileScanRDD = {
    MethodUtils.invokeMethod(basedFileScan, true, "prepare")
    MethodUtils.invokeMethod(basedFileScan, true, "waitForSubqueries")
    basedFileScan.inputRDDs().head match {
      case rdd: FileScanRDD => rdd
      case rdd: MapPartitionsRDD[_, _] => rdd.prev.asInstanceOf[FileScanRDD]
    }
  }

  private val partitionSchema = basedFileScan.relation.partitionSchema

  private val fileSizes = inputFileScanRDD.filePartitions
    .flatMap(_.files)
    .groupBy(_.filePath)
    .mapValues(_.map(_.length).sum)
    .map(identity) // make this map serializable

  private def nativePruningPredicateFilters = basedFileScan.dataFilters
    .map(expr => NativeConverters.convertScanPruningExpr(expr))

  private def nativeFileSchema =
    NativeConverters.convertSchema(StructType(basedFileScan.relation.dataSchema.map {
      case field if basedFileScan.requiredSchema.exists(_.name == field.name) =>
        field.copy(nullable = true)
      case field =>
        // avoid converting unsupported type in non-used fields
        StructField(field.name, NullType, nullable = true)
    }))

  private def nativePartitionSchema =
    NativeConverters.convertSchema(partitionSchema)

  private def nativeFileGroups = (partition: FilePartition) => {
    // list input file statuses
    val nativePartitionedFile = (file: PartitionedFile) => {
      val nativePartitionValues = partitionSchema.zipWithIndex.map { case (field, index) =>
        NativeConverters.convertValue(
          file.partitionValues.get(index, field.dataType),
          field.dataType)
      }
      pb.PartitionedFile
        .newBuilder()
        .setPath(file.filePath)
        .setSize(fileSizes(file.filePath))
        .addAllPartitionValues(nativePartitionValues.asJava)
        .setLastModifiedNs(0)
        .setRange(
          pb.FileRange
            .newBuilder()
            .setStart(file.start)
            .setEnd(file.start + file.length)
            .build())
        .build()
    }
    pb.FileGroup
      .newBuilder()
      .addAllFiles(partition.files.map(nativePartitionedFile).toList.asJava)
      .build()
  }

  // check whether native converting is supported
  nativePruningPredicateFilters
  nativeFileSchema
  nativePartitionSchema
  nativeFileGroups

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
    val sparkSession = Shims.get.getSqlContext(basedFileScan).sparkSession
    val hadoopConf =
      sparkSession.sessionState.newHadoopConfWithOptions(basedFileScan.relation.options)
    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    val numPartitions = partitions.length

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      partitions.asInstanceOf[Array[Partition]],
      Nil,
      rddShuffleReadFull = true,
      (partition, _context) => {
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

        val nativeFileGroup = nativeFileGroups(partition.asInstanceOf[FilePartition])
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

  override val nodeName: String =
    s"NativeParquetScan ${basedFileScan.tableIdentifier.map(_.unquotedString).getOrElse("")}"

  override protected def doCanonicalize(): SparkPlan = basedFileScan.canonicalized
}
