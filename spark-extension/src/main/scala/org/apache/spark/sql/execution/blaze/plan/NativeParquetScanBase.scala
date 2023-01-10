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
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.datasources.FileScanRDD
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.MapPartitionsRDD
import org.apache.spark.sql.blaze.JniBridge
import org.apache.spark.sql.blaze.MetricNode
import org.apache.spark.sql.blaze.NativeConverters
import org.apache.spark.sql.blaze.NativeRDD
import org.apache.spark.sql.blaze.NativeHelper
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types.NullType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration
import org.blaze.{protobuf => pb}
import org.blaze.protobuf.FileGroup
import org.apache.spark.sql.blaze.NativeSupports

abstract class NativeParquetScanBase(basedFileScan: FileSourceScanExec)
    extends LeafExecNode
    with NativeSupports {

  override lazy val metrics: Map[String, SQLMetric] = Map(
    NativeHelper
      .getDefaultNativeMetrics(sparkContext)
      .filterKeys(Set("output_rows", "elapsed_compute"))
      .toSeq :+
      ("predicate_evaluation_errors", SQLMetrics
        .createMetric(sparkContext, "Native.predicate_evaluation_errors")) :+
      ("row_groups_pruned", SQLMetrics.createMetric(sparkContext, "Native.row_groups_pruned")) :+
      ("bytes_scanned", SQLMetrics.createSizeMetric(sparkContext, "Native.bytes_scanned")) :+
      ("io_time", SQLMetrics.createNanoTimingMetric(sparkContext, "Native.io_time")): _*)

  override val output: Seq[Attribute] = basedFileScan.output
  override val outputPartitioning: Partitioning = basedFileScan.outputPartitioning

  private val inputFileScanRDD = {
    basedFileScan.inputRDDs().head match {
      case rdd: FileScanRDD => rdd
      case rdd: MapPartitionsRDD[_, _] => rdd.prev.asInstanceOf[FileScanRDD]
    }
  }

  private val nativePruningPredicateFilters = basedFileScan.dataFilters
    .map(expr => NativeConverters.convertExpr(expr, useAttrExprId = false))

  private val nativeFileSchema =
    NativeConverters.convertSchema(StructType(basedFileScan.relation.dataSchema.map {
      case field if basedFileScan.requiredSchema.exists(_.name == field.name) => field
      case field =>
        // avoid converting unsupported type in non-used fields
        StructField(field.name, NullType)
    }))
  private val nativeFileGroups: Array[pb.FileGroup] = {
    val partitions = inputFileScanRDD.filePartitions.toArray

    // compute file sizes
    val fileSizes = partitions
      .flatMap(_.files)
      .groupBy(_.filePath)
      .mapValues(_.map(_.length).sum)

    // list input file statuses
    def nativePartitionedFile(file: PartitionedFile) = {
      val nativePartitionValues =
        basedFileScan.relation.partitionSchema.zipWithIndex
          .map {
            case (field, index) =>
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

    partitions.map { partition =>
      pb.FileGroup
        .newBuilder()
        .addAllFiles(partition.files.map(nativePartitionedFile).toList.asJava)
        .build()
    }
  }

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
    val projection = schema.map(field => basedFileScan.relation.schema.fieldIndex(field.name))
    val partCols = basedFileScan.relation.partitionSchema.map(_.name)
    val partitionSchema = NativeConverters.convertSchema(basedFileScan.relation.partitionSchema)

    val relation = basedFileScan.relation
    val sparkSession = relation.sparkSession
    val hadoopConf =
      sparkSession.sessionState.newHadoopConfWithOptions(basedFileScan.relation.options)
    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      partitions.asInstanceOf[Array[Partition]],
      Nil,
      rddShuffleReadFull = true,
      (partition, _) => {
        val resourceId = s"NativeParquetScanExec:${UUID.randomUUID().toString}"
        val sharedConf = broadcastedHadoopConf.value.value
        JniBridge.resourcesMap.put(
          resourceId,
          (location: String) => {
            NativeHelper.currentUser.doAs(new PrivilegedExceptionAction[FileSystem] {
              override def run(): FileSystem = {
                FileSystem.get(new URI(location), sharedConf)
              }
            })
          })

        val nativeFileGroupsPartitioned = nativeFileGroups.zipWithIndex
          .map {
            case (fileGroup, index) if index == partition.index => fileGroup
            case (_, _) => FileGroup.getDefaultInstance
          }

        val nativeParquetScanConf = pb.FileScanExecConf
          .newBuilder()
          .setStatistics(pb.Statistics.getDefaultInstance)
          .setSchema(nativeFileSchema)
          .addAllFileGroups(nativeFileGroupsPartitioned.toList.asJava)
          .addAllProjection(projection.map(Integer.valueOf).asJava)
          .addAllTablePartitionCols(partCols.asJava)
          .setPartitionSchema(partitionSchema)
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

  override def doCanonicalize(): SparkPlan = basedFileScan.canonicalized
}
