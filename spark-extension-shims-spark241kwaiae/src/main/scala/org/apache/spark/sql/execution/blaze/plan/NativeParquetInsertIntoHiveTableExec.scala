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

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.blaze.Shims
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.BasicWriteJobStatsTracker
import org.apache.spark.sql.execution.datasources.BasicWriteTaskStats
import org.apache.spark.sql.execution.datasources.PartitionWriteJobStatsTracker
import org.apache.spark.sql.execution.datasources.WriteTaskStats
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.execution.datasources.BasicWriteTaskStatsTracker
import org.apache.spark.sql.execution.datasources.WriteTaskStatsTracker
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.datasources.PartitionWriteTaskStatsTracker
import org.apache.spark.util.SerializableConfiguration

case class NativeParquetInsertIntoHiveTableExec(
    cmd: InsertIntoHiveTable,
    override val child: SparkPlan)
    extends NativeParquetInsertIntoHiveTableBase(cmd, child) {

  def check2(): Unit = {
    val tblStorage = cmd.table.storage
    assert(tblStorage.bucketCols.isEmpty, "not supported writing bucketed table")
    assert(tblStorage.sortCols.isEmpty, "not supported writing table with sorted columns")
  }
  check2()

  override protected def getInsertIntoHiveTableCommand(
      table: CatalogTable,
      partition: Map[String, Option[String]],
      query: LogicalPlan,
      overwrite: Boolean,
      ifPartitionNotExists: Boolean,
      outputColumnNames: Seq[String],
      metrics: Map[String, SQLMetric]): InsertIntoHiveTable = new BlazeInsertIntoHiveTable(
    table,
    partition,
    query,
    overwrite,
    ifPartitionNotExists,
    outputColumnNames,
    metrics)

  override def withNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
    copy(child = newChildren.head)
}

class BlazeInsertIntoHiveTable(
    table: CatalogTable,
    partition: Map[String, Option[String]],
    query: LogicalPlan,
    overwrite: Boolean,
    ifPartitionNotExists: Boolean,
    outputColumnNames: Seq[String],
    outerMetrics: Map[String, SQLMetric])
    extends InsertIntoHiveTable(
      table,
      partition,
      query,
      overwrite,
      ifPartitionNotExists,
      outputColumnNames) {

  override lazy val metrics: Map[String, SQLMetric] = outerMetrics

  override def run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] = {
    val nativeParquetSink =
      Shims.get.createNativeParquetSinkExec(sparkSession, table, partition, child, metrics)
    super.run(sparkSession, nativeParquetSink)
  }

  override def basicWriteJobStatsTracker(hadoopConf: Configuration): BasicWriteJobStatsTracker = {
    val serializableHadoopConf = new SerializableConfiguration(hadoopConf)
    new BasicWriteJobStatsTracker(serializableHadoopConf, metrics) {
      override def newTaskInstance(): WriteTaskStatsTracker = {
        new BasicWriteTaskStatsTracker(serializableHadoopConf.value) {
          override def newRow(row: InternalRow): Unit = {}
          override def getFinalStats(): WriteTaskStats = {
            val outputFileStat = ParquetSinkTaskContext.get.processedOutputFiles.remove()
            new BasicWriteTaskStats(
              numPartitions = 1,
              numFiles = 1,
              numBytes = outputFileStat.numBytes,
              numRows = outputFileStat.numRows,
              mergeV1 = 0,
              mergeV2 = 0)
          }
        }
      }
    }
  }

  override def partitionWriteJobStatsTracker(
      hadoopConf: Configuration,
      partitionAttributes: Seq[Attribute],
      staticPartition: Map[String, Option[String]],
      partitionSchema: StructType): BasicWriteJobStatsTracker = {

    val serializableHadoopConf = new SerializableConfiguration(hadoopConf)
    new PartitionWriteJobStatsTracker(
      serializableHadoopConf,
      partitionAttributes,
      metrics,
      staticPartition,
      partitionSchema) {

      override def newTaskInstance(): WriteTaskStatsTracker = {
        new PartitionWriteTaskStatsTracker(
          partitionAttributes,
          serializableHadoopConf.value,
          staticPartition,
          partitionSchema) {

          override def newRow(row: InternalRow): Unit = {}

          override def newPartition(partitionValues: InternalRow): Unit = {
            fillPartitionNumRowsIfExists()
            super.newPartition(partitionValues)
          }

          override def getFinalStats(): WriteTaskStats = {
            fillPartitionNumRowsIfExists()
            super.getFinalStats()
          }

          private var firstPartition = true

          private def fillPartitionNumRowsIfExists(): Unit = {
            if (!firstPartition) {
              // set correct value of numRows of previous partition
              val outputFileStat = ParquetSinkTaskContext.get.processedOutputFiles.pop()
              super.newRows(outputFileStat.numRows)
            }
            firstPartition = false
          }
        }
      }
    }
  }
}
