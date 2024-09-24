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

import org.apache.spark.sql.blaze.Shims
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import com.thoughtworks.enableIf

case class NativeParquetInsertIntoHiveTableExec(
    cmd: InsertIntoHiveTable,
    override val child: SparkPlan)
    extends NativeParquetInsertIntoHiveTableBase(cmd, child) {

  @enableIf(
    Seq("spark-3.0", "spark-3.1", "spark-3.2", "spark-3.3").contains(
      System.getProperty("blaze.shim")))
  override protected def getInsertIntoHiveTableCommand(
      table: CatalogTable,
      partition: Map[String, Option[String]],
      query: LogicalPlan,
      overwrite: Boolean,
      ifPartitionNotExists: Boolean,
      outputColumnNames: Seq[String],
      metrics: Map[String, SQLMetric]): InsertIntoHiveTable = {
    new BlazeInsertIntoHiveTable303(
      table,
      partition,
      query,
      overwrite,
      ifPartitionNotExists,
      outputColumnNames,
      metrics)
  }

  @enableIf(Seq("spark-3.5").contains(System.getProperty("blaze.shim")))
  override protected def getInsertIntoHiveTableCommand(
      table: CatalogTable,
      partition: Map[String, Option[String]],
      query: LogicalPlan,
      overwrite: Boolean,
      ifPartitionNotExists: Boolean,
      outputColumnNames: Seq[String],
      metrics: Map[String, SQLMetric]): InsertIntoHiveTable = {
    new BlazeInsertIntoHiveTable351(
      table,
      partition,
      query,
      overwrite,
      ifPartitionNotExists,
      outputColumnNames,
      metrics)
  }

  @enableIf(Seq("spark-3.2", "spark-3.3", "spark-3.5").contains(System.getProperty("blaze.shim")))
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)

  @enableIf(Seq("spark-3.0", "spark-3.1").contains(System.getProperty("blaze.shim")))
  override def withNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
    copy(child = newChildren.head)

  @enableIf(
    Seq("spark-3.0", "spark-3.1", "spark-3.2", "spark-3.3").contains(
      System.getProperty("blaze.shim")))
  class BlazeInsertIntoHiveTable303(
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

    @enableIf(Seq("spark-3.2", "spark-3.3").contains(System.getProperty("blaze.shim")))
    override def basicWriteJobStatsTracker(hadoopConf: org.apache.hadoop.conf.Configuration) = {
      import org.apache.spark.sql.catalyst.InternalRow
      import org.apache.spark.sql.execution.datasources.BasicWriteJobStatsTracker
      import org.apache.spark.sql.execution.datasources.BasicWriteTaskStatsTracker
      import org.apache.spark.sql.execution.datasources.WriteTaskStatsTracker
      import org.apache.spark.util.SerializableConfiguration

      val serializableHadoopConf = new SerializableConfiguration(hadoopConf)
      new BasicWriteJobStatsTracker(serializableHadoopConf, metrics) {
        override def newTaskInstance(): WriteTaskStatsTracker = {
          new BasicWriteTaskStatsTracker(serializableHadoopConf.value) {
            override def newRow(_filePath: String, _row: InternalRow): Unit = {}

            override def closeFile(filePath: String): Unit = {
              val outputFileStat = ParquetSinkTaskContext.get.processedOutputFiles.remove()
              for (_ <- 0L until outputFileStat.numRows) {
                super.newRow(filePath, null)
              }
              super.closeFile(filePath)
            }
          }
        }
      }
    }

    @enableIf(Seq("spark-3.1").contains(System.getProperty("blaze.shim")))
    override def basicWriteJobStatsTracker(hadoopConf: org.apache.hadoop.conf.Configuration) = {
      import org.apache.spark.sql.catalyst.InternalRow
      import org.apache.spark.sql.execution.datasources.BasicWriteJobStatsTracker
      import org.apache.spark.sql.execution.datasources.BasicWriteTaskStats
      import org.apache.spark.sql.execution.datasources.BasicWriteTaskStatsTracker
      import org.apache.spark.sql.execution.datasources.WriteTaskStats
      import org.apache.spark.sql.execution.datasources.WriteTaskStatsTracker
      import org.apache.spark.util.SerializableConfiguration

      import scala.collection.mutable

      val serializableHadoopConf = new SerializableConfiguration(hadoopConf)
      new BasicWriteJobStatsTracker(serializableHadoopConf, metrics) {
        override def newTaskInstance(): WriteTaskStatsTracker = {
          new BasicWriteTaskStatsTracker(serializableHadoopConf.value) {
            private[this] val partitions: mutable.ArrayBuffer[InternalRow] =
              mutable.ArrayBuffer.empty

            override def newPartition(partitionValues: InternalRow): Unit = {
              partitions.append(partitionValues)
            }

            override def newRow(_row: InternalRow): Unit = {}

            override def getFinalStats(): WriteTaskStats = {
              val outputFileStat = ParquetSinkTaskContext.get.processedOutputFiles.remove()
              BasicWriteTaskStats(
                partitions = partitions,
                numFiles = 1,
                numBytes = outputFileStat.numBytes,
                numRows = outputFileStat.numRows)
            }
          }
        }
      }
    }

    @enableIf(Seq("spark-3.0").contains(System.getProperty("blaze.shim")))
    override def basicWriteJobStatsTracker(hadoopConf: org.apache.hadoop.conf.Configuration) = {
      import org.apache.spark.sql.catalyst.InternalRow
      import org.apache.spark.sql.execution.datasources.BasicWriteJobStatsTracker
      import org.apache.spark.sql.execution.datasources.BasicWriteTaskStats
      import org.apache.spark.sql.execution.datasources.BasicWriteTaskStatsTracker
      import org.apache.spark.sql.execution.datasources.WriteTaskStats
      import org.apache.spark.sql.execution.datasources.WriteTaskStatsTracker
      import org.apache.spark.util.SerializableConfiguration

      val serializableHadoopConf = new SerializableConfiguration(hadoopConf)
      new BasicWriteJobStatsTracker(serializableHadoopConf, metrics) {
        override def newTaskInstance(): WriteTaskStatsTracker = {
          new BasicWriteTaskStatsTracker(serializableHadoopConf.value) {
            override def newRow(_row: InternalRow): Unit = {}

            override def getFinalStats(): WriteTaskStats = {
              val outputFileStat = ParquetSinkTaskContext.get.processedOutputFiles.remove()
              BasicWriteTaskStats(
                numPartitions = 1,
                numFiles = 1,
                numBytes = outputFileStat.numBytes,
                numRows = outputFileStat.numRows)
            }
          }
        }
      }
    }
  }

  @enableIf(Seq("spark-3.5").contains(System.getProperty("blaze.shim")))
  class BlazeInsertIntoHiveTable351(
      table: CatalogTable,
      partition: Map[String, Option[String]],
      query: LogicalPlan,
      overwrite: Boolean,
      ifPartitionNotExists: Boolean,
      outputColumnNames: Seq[String],
      outerMetrics: Map[String, SQLMetric])
      extends {
        private val insertIntoHiveTable = InsertIntoHiveTable(
          table,
          partition,
          query,
          overwrite,
          ifPartitionNotExists,
          outputColumnNames)
        private val initPartitionColumns = insertIntoHiveTable.partitionColumns
        private val initBucketSpec = insertIntoHiveTable.bucketSpec
        private val initOptions = insertIntoHiveTable.options
        private val initFileFormat = insertIntoHiveTable.fileFormat
        private val initHiveTmpPath = insertIntoHiveTable.hiveTmpPath

      }
      with InsertIntoHiveTable(
        table,
        partition,
        query,
        overwrite,
        ifPartitionNotExists,
        outputColumnNames,
        initPartitionColumns,
        initBucketSpec,
        initOptions,
        initFileFormat,
        initHiveTmpPath) {

    override lazy val metrics: Map[String, SQLMetric] = outerMetrics

    override def run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] = {
      val nativeParquetSink =
        Shims.get.createNativeParquetSinkExec(sparkSession, table, partition, child, metrics)
      super.run(sparkSession, nativeParquetSink)
    }
  }
}
