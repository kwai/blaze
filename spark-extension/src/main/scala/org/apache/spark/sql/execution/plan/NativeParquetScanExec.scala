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

package org.apache.spark.sql.execution.plan

import scala.collection.JavaConverters._

import org.apache.spark.sql.blaze.MetricNode
import org.apache.spark.sql.blaze.NativeConverters
import org.apache.spark.sql.blaze.NativeRDD
import org.apache.spark.sql.blaze.NativeSupports
import org.apache.spark.sql.catalyst.expressions.And
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.datasources.FileScanRDD
import org.apache.spark.Partition
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.SparkPlan
import org.blaze.protobuf.FileGroup
import org.blaze.protobuf.FileRange
import org.blaze.protobuf.FileScanExecConf
import org.blaze.protobuf.ParquetScanExecNode
import org.blaze.protobuf.PartitionedFile
import org.blaze.protobuf.PhysicalPlanNode
import org.blaze.protobuf.Statistics

case class NativeParquetScanExec(basedFileScan: FileSourceScanExec)
    extends LeafExecNode
    with NativeSupports {

  override lazy val metrics: Map[String, SQLMetric] =
    NativeSupports.getDefaultNativeMetrics(sparkContext)

  override def output: Seq[Attribute] = basedFileScan.output
  override def outputPartitioning: Partitioning = basedFileScan.outputPartitioning

  private val inputFileScanRDD = basedFileScan.inputRDD.asInstanceOf[FileScanRDD]

  private val nativeFileSchema = NativeConverters.convertSchema(basedFileScan.relation.schema)
  private val nativePruningPredicateFilter = basedFileScan.dataFilters
    .reduceOption(And)
    .map(NativeConverters.convertExprLogical)

  private val nativeFileGroups: Array[FileGroup] = {
    val partitions = inputFileScanRDD.filePartitions.toArray

    // compute file sizes
    val fileSizes = partitions
      .flatMap(_.files)
      .groupBy(_.filePath)
      .mapValues(_.map(_.length).sum)

    // list input file statuses
    def nativePartitionedFile(file: org.apache.spark.sql.execution.datasources.PartitionedFile) =
      PartitionedFile
        .newBuilder()
        .setPath(file.filePath.replaceFirst("^file://", ""))
        .setSize(fileSizes(file.filePath))
        .setLastModifiedNs(0)
        .setRange(
          FileRange
            .newBuilder()
            .setStart(file.start)
            .setEnd(file.start + file.length)
            .build())
        .build()

    partitions.map { partition =>
      FileGroup
        .newBuilder()
        .addAllFiles(partition.files.map(nativePartitionedFile).toList.asJava)
        .build()
    }
  }

  override def doExecuteNative(): NativeRDD = {
    val partitions = inputFileScanRDD.filePartitions.toArray
    val nativeMetrics = MetricNode(metrics, Nil)

    val fileSchema = basedFileScan.relation.schema
    val outputSchema = basedFileScan.requiredSchema.fields
    val projection = outputSchema.map(field => fileSchema.fieldIndex(field.name))

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      partitions.asInstanceOf[Array[Partition]],
      Nil,
      (partition, _) => {
        val nativeParquetScanConf = FileScanExecConf
          .newBuilder()
          .setStatistics(Statistics.getDefaultInstance)
          .setSchema(nativeFileSchema)
          .addAllFileGroups(nativeFileGroups.toList.asJava)
          .addAllProjection(projection.map(Integer.valueOf).toSeq.asJava)
          .build()

        val nativeParquetScanExecBuilder = ParquetScanExecNode
          .newBuilder()
          .setBaseConf(nativeParquetScanConf)

        nativePruningPredicateFilter match {
          case Some(filter) => nativeParquetScanExecBuilder.setPruningPredicate(filter)
          case None => // no nothing
        }

        PhysicalPlanNode
          .newBuilder()
          .setParquetScan(nativeParquetScanExecBuilder.build())
          .build()
      })
  }

  override val nodeName: String =
    s"NativeParquetScan ${basedFileScan.tableIdentifier.map(_.unquotedString).getOrElse("")}"

  override def simpleString(maxFields: Int): String =
    s"$nodeName (${basedFileScan.simpleString(maxFields)})"

  override def doCanonicalize(): SparkPlan = basedFileScan.canonicalized
}
