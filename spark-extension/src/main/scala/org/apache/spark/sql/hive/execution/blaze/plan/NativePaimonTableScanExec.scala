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
package org.apache.spark.sql.hive.execution.blaze.plan

import java.util.UUID
import scala.collection.JavaConverters._
import scala.collection.Seq

import org.apache.paimon.io.DataFileMeta
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.source.DataSplit
import org.apache.paimon.utils.RowDataToObjectArrayConverter
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.blaze.MetricNode
import org.apache.spark.sql.blaze.NativeRDD
import org.apache.spark.sql.blaze.Shims
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.hive.blaze.PaimonUtil
import org.apache.spark.sql.hive.execution.HiveTableScanExec
import org.apache.spark.sql.types.StructType
import org.blaze.{protobuf => pb}

case class NativePaimonTableScanExec(basedHiveScan: HiveTableScanExec)
    extends NativeHiveTableScanBase(basedHiveScan)
    with Logging {

  private lazy val table: FileStoreTable =
    PaimonUtil.loadTable(relation.tableMeta.location.toString)
  private lazy val fileFormat = PaimonUtil.paimonFileFormat(table)

  override def doExecuteNative(): NativeRDD = {
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
    // val nativePruningPredicateFilters = this.nativePruningPredicateFilters
    val nativeFileSchema = this.nativeFileSchema
    val nativeFileGroups = this.nativeFileGroups
    val nativePartitionSchema = this.nativePartitionSchema

    val projection = schema.map(field => relation.schema.fieldIndex(field.name))
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
        val resourceId = s"NativePaimonTableScan:${UUID.randomUUID().toString}"
        putJniBridgeResource(resourceId, broadcastedHadoopConf)

        val nativeFileGroup = nativeFileGroups(partition.asInstanceOf[FilePartition])
        val nativeFileScanConf = pb.FileScanExecConf
          .newBuilder()
          .setNumPartitions(numPartitions)
          .setPartitionIndex(partition.index)
          .setStatistics(pb.Statistics.getDefaultInstance)
          .setSchema(nativeFileSchema)
          .setFileGroup(nativeFileGroup)
          .addAllProjection(projection.map(Integer.valueOf).asJava)
          .setPartitionSchema(nativePartitionSchema)
          .build()
        if (fileFormat.equalsIgnoreCase(PaimonUtil.orcFormat)) {
          val nativeOrcScanExecBuilder = pb.OrcScanExecNode
            .newBuilder()
            .setBaseConf(nativeFileScanConf)
            .setFsResourceId(resourceId)
            .addAllPruningPredicates(new java.util.ArrayList()) // not support this filter

          pb.PhysicalPlanNode
            .newBuilder()
            .setOrcScan(nativeOrcScanExecBuilder.build())
            .build()
        } else {
          val nativeParquetScanExecBuilder = pb.ParquetScanExecNode
            .newBuilder()
            .setBaseConf(nativeFileScanConf)
            .setFsResourceId(resourceId)
            .addAllPruningPredicates(new java.util.ArrayList()) // not support this filter

          pb.PhysicalPlanNode
            .newBuilder()
            .setParquetScan(nativeParquetScanExecBuilder.build())
            .build()
        }
      },
      friendlyName = "NativeRDD.PaimonScan")
  }

  override val nodeName: String =
    s"NativePaimonTableScan $tableName"

  override def getFilePartitions(): Array[FilePartition] = {
    val currentTimeMillis = System.currentTimeMillis()
    val sparkSession = Shims.get.getSqlContext(basedHiveScan).sparkSession
    // TODO: Verify paimon cow table without level0 and deleted row in DataSplit and all DataFileMetas are same level
    val splits =
      table.newScan().plan().splits().asScala.map(split => split.asInstanceOf[DataSplit])
    logInfo(
      s"Get paimon table $tableName splits elapse: ${System.currentTimeMillis() - currentTimeMillis} ms")

    val dataSplitPartitions = if (relation.isPartitioned) {
      val rowDataToObjectArrayConverter = new RowDataToObjectArrayConverter(
        table.schema().logicalPartitionType())
      val sessionLocalTimeZone = sparkSession.sessionState.conf.sessionLocalTimeZone
      if (relation.prunedPartitions.nonEmpty) {
        val partitionPathAndValues =
          relation.prunedPartitions.get.map { catalogTablePartition =>
            (
              catalogTablePartition.spec.map { case (k, v) => s"$k=$v" }.mkString("/"),
              catalogTablePartition.toRow(partitionSchema, sessionLocalTimeZone))
          }.toMap
        val partitionKeys = table.schema().partitionKeys()
        // pruning paimon splits
        splits
          .map { split =>
            val values = rowDataToObjectArrayConverter.convert(split.partition())
            val partitionPath = values.zipWithIndex
              .map { case (v, i) => s"${partitionKeys.get(i)}=${v.toString}" }
              .mkString("/")
            (split, partitionPathAndValues.getOrElse(partitionPath, null))
          }
          .filter(_._2 != null)
      } else {
        // don't prune partitions
        // fork {@link CatalogDatabase#toRow}
        def toRow(
            values: Seq[String],
            partitionSchema: StructType,
            defaultTimeZondId: String): InternalRow = {
          val caseInsensitiveProperties = CaseInsensitiveMap(
            relation.tableMeta.storage.properties)
          val timeZoneId =
            caseInsensitiveProperties.getOrElse(DateTimeUtils.TIMEZONE_OPTION, defaultTimeZondId)
          InternalRow.fromSeq(partitionSchema.zipWithIndex.map { case (field, index) =>
            val partValue = if (values(index) == ExternalCatalogUtils.DEFAULT_PARTITION_NAME) {
              null
            } else {
              values(index)
            }
            Cast(Literal(partValue), field.dataType, Option(timeZoneId)).eval()
          })
        }
        splits.map { split =>
          val values = rowDataToObjectArrayConverter.convert(split.partition()).map(_.toString)
          (split, toRow(values, partitionSchema, sessionLocalTimeZone))
        }
      }
    } else {
      splits.map((_, InternalRow.empty))
    }
    logInfo(
      s"Table: $tableName, total splits: ${splits.length}, selected splits: ${dataSplitPartitions.length}")

    val isSplitable =
      fileFormat.equalsIgnoreCase(PaimonUtil.parquetFormat) || fileFormat.equalsIgnoreCase(
        PaimonUtil.orcFormat)
    val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
    val maxSplitBytes = getMaxSplitBytes(sparkSession, dataSplitPartitions.map(_._1))
    logInfo(
      s"Planning scan with bin packing, max size: $maxSplitBytes bytes, " +
        s"open cost is considered as scanning $openCostInBytes bytes.")
    val partitionedFiles = dataSplitPartitions
      .flatMap { partition =>
        partition._1.dataFiles().asScala.flatMap { dataFileMeta =>
          val filePath = s"${partition._1.bucketPath()}/${dataFileMeta.fileName()}"
          splitFiles(dataFileMeta, filePath, isSplitable, maxSplitBytes, partition._2)
        }
      }
      .sortBy(_.length)(implicitly[Ordering[Long]].reverse)
    FilePartition.getFilePartitions(sparkSession, partitionedFiles, maxSplitBytes).toArray
  }

  // fork {@link PartitionedFileUtil#splitFiles}
  private def splitFiles(
      dataFileMeta: DataFileMeta,
      filePath: String,
      isSplitable: Boolean,
      maxSplitBytes: Long,
      partitionValues: InternalRow): Seq[PartitionedFile] = {
    if (isSplitable) {
      (0L until dataFileMeta.fileSize() by maxSplitBytes).map { offset =>
        val remaining = dataFileMeta.fileSize() - offset
        val size = if (remaining > maxSplitBytes) maxSplitBytes else remaining
        Shims.get.getPartitionedFile(partitionValues, filePath, offset, size)
      }
    } else {
      Seq(Shims.get.getPartitionedFile(partitionValues, filePath, 0, dataFileMeta.fileSize()))
    }
  }

  // fork {@link FilePartition#maxSplitBytes}
  private def getMaxSplitBytes(
      sparkSession: SparkSession,
      selectedSplits: Seq[DataSplit]): Long = {
    val defaultMaxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes
    val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
    val minPartitionNum = Shims.get.getMinPartitionNum(sparkSession)
    val totalBytes = selectedSplits
      .flatMap(_.dataFiles().asScala.map(_.fileSize() + openCostInBytes))
      .sum
    val bytesPerCore = totalBytes / minPartitionNum

    Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore))
  }
}
