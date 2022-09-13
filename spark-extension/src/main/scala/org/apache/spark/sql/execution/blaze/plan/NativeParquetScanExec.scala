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

import java.util.Base64

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.expressions.And
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.datasources.FileScanRDD
import org.apache.spark.Partition
import org.apache.spark.rdd.MapPartitionsRDD
import org.apache.spark.sql.blaze.MetricNode
import org.apache.spark.sql.blaze.NativeConverters
import org.apache.spark.sql.blaze.NativeRDD
import org.apache.spark.sql.blaze.NativeSupports
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.blaze.{protobuf => pb}

case class NativeParquetScanExec(basedFileScan: FileSourceScanExec)
    extends LeafExecNode
    with NativeSupports {

  override lazy val metrics: Map[String, SQLMetric] =
    NativeSupports.getDefaultNativeMetrics(sparkContext)

  override def output: Seq[Attribute] = basedFileScan.output
  override def outputPartitioning: Partitioning = basedFileScan.outputPartitioning

  private val inputFileScanRDD = {
    basedFileScan.inputRDDs().head match {
      case rdd: FileScanRDD => rdd
      case rdd: MapPartitionsRDD[_, _] => rdd.prev.asInstanceOf[FileScanRDD]
    }
  }

  private val nativePruningPredicateFilter = basedFileScan.dataFilters
    .reduceOption(And)
    .map(NativeConverters.convertExpr)

  private val nativeFileSchema = NativeConverters.convertSchema(basedFileScan.relation.dataSchema)
  private val nativeFileGroups: Array[pb.FileGroup] = {
    val partitions = inputFileScanRDD.filePartitions.toArray

    // compute file sizes
    val fileSizes = partitions
      .flatMap(_.files)
      .groupBy(_.filePath)
      .mapValues(_.map(_.length).sum)

    def formatFilePath(path: String): String = {
      if (path.startsWith("hdfs://") || path.startsWith("viewfs://") || path.startsWith(
          "har://")) {
        // NOTE: see native object store registration codes
        return s"hdfs://-/${Base64.getEncoder.encodeToString(path.getBytes)}"
      }
      path
    }

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
        .setPath(formatFilePath(file.filePath))
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
    val nativeMetrics = MetricNode(metrics, Nil)
    val projection = schema.map(field => basedFileScan.relation.schema.fieldIndex(field.name))
    val partCols = basedFileScan.relation.partitionSchema.map(_.name)
    val partitionSchema = NativeConverters.convertSchema(basedFileScan.relation.partitionSchema)

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      partitions.asInstanceOf[Array[Partition]],
      Nil,
      rddShuffleReadFull = true,
      (_, _) => {
        val nativeParquetScanConf = pb.FileScanExecConf
          .newBuilder()
          .setStatistics(pb.Statistics.getDefaultInstance)
          .setSchema(nativeFileSchema)
          .addAllFileGroups(nativeFileGroups.toList.asJava)
          .addAllProjection(projection.map(Integer.valueOf).asJava)
          .addAllTablePartitionCols(partCols.asJava)
          .setPartitionSchema(partitionSchema)
          .build()

        val nativeParquetScanExecBuilder = pb.ParquetScanExecNode
          .newBuilder()
          .setBaseConf(nativeParquetScanConf)

        nativePruningPredicateFilter match {
          case Some(filter) => nativeParquetScanExecBuilder.setPruningPredicate(filter)
          case None => // no nothing
        }

        pb.PhysicalPlanNode
          .newBuilder()
          .setParquetScan(nativeParquetScanExecBuilder.build())
          .build()
      },
      friendlyName = "NativeRDD.ParquetScan")
  }

  override val nodeName: String =
    s"NativeParquetScan ${basedFileScan.tableIdentifier.map(_.unquotedString).getOrElse("")}"

  def simpleString(maxFields: Int): String =
    s"$nodeName (${basedFileScan.simpleString(maxFields)})"

  override def doCanonicalize(): SparkPlan = basedFileScan.canonicalized
}
