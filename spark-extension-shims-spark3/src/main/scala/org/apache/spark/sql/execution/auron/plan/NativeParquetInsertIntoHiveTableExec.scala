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
package org.apache.spark.sql.execution.auron.plan

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.auron.Shims
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable

import org.apache.auron.sparkver

case class NativeParquetInsertIntoHiveTableExec(
    cmd: InsertIntoHiveTable,
    override val child: SparkPlan)
    extends NativeParquetInsertIntoHiveTableBase(cmd, child) {

  @sparkver("3.0 / 3.1 / 3.2 / 3.3")
  override protected def getInsertIntoHiveTableCommand(
      table: CatalogTable,
      partition: Map[String, Option[String]],
      query: LogicalPlan,
      overwrite: Boolean,
      ifPartitionNotExists: Boolean,
      outputColumnNames: Seq[String],
      metrics: Map[String, SQLMetric]): InsertIntoHiveTable = {
    new AuronInsertIntoHiveTable30(
      table,
      partition,
      query,
      overwrite,
      ifPartitionNotExists,
      outputColumnNames,
      metrics)
  }

  @sparkver("3.4 / 3.5")
  override protected def getInsertIntoHiveTableCommand(
      table: CatalogTable,
      partition: Map[String, Option[String]],
      query: LogicalPlan,
      overwrite: Boolean,
      ifPartitionNotExists: Boolean,
      outputColumnNames: Seq[String],
      metrics: Map[String, SQLMetric]): InsertIntoHiveTable = {
    new AuronInsertIntoHiveTable34(
      table,
      partition,
      query,
      overwrite,
      ifPartitionNotExists,
      outputColumnNames,
      metrics)
  }

  @sparkver("3.2 / 3.3 / 3.4 / 3.5")
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)

  @sparkver("3.0 / 3.1")
  override def withNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
    copy(child = newChildren.head)

  @sparkver("3.0 / 3.1 / 3.2 / 3.3")
  class AuronInsertIntoHiveTable30(
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

    @sparkver("3.2 / 3.3")
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
            override def newRow(filePath: String, row: InternalRow): Unit = {
              if (!ParquetSinkTaskContext.get.isNative) {
                return super.newRow(filePath, row)
              }
            }

            override def closeFile(filePath: String): Unit = {
              if (!ParquetSinkTaskContext.get.isNative) {
                return super.closeFile(filePath)
              }

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

    @sparkver("3.1")
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
              if (!ParquetSinkTaskContext.get.isNative) {
                return super.newPartition(partitionValues)
              }
              partitions.append(partitionValues)
            }

            override def newRow(row: InternalRow): Unit = {
              if (!ParquetSinkTaskContext.get.isNative) {
                return super.newRow(row)
              }
            }

            override def getFinalStats(): WriteTaskStats = {
              if (!ParquetSinkTaskContext.get.isNative) {
                return super.getFinalStats()
              }

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

    @sparkver("3.0")
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
            override def newRow(row: InternalRow): Unit = {
              if (!ParquetSinkTaskContext.get.isNative) {
                return super.newRow(row)
              }
            }

            override def getFinalStats(): WriteTaskStats = {
              if (!ParquetSinkTaskContext.get.isNative) {
                return super.getFinalStats()
              }

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

  @sparkver("3.4 / 3.5")
  class AuronInsertIntoHiveTable34(
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
