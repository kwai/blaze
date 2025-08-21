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

import java.util
import java.util.Locale
import java.util.Properties
import java.util.concurrent.LinkedBlockingDeque

import scala.collection.immutable.SortedMap
import scala.collection.mutable

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.FileSinkOperator
import org.apache.hadoop.hive.ql.io.HiveOutputFormat
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.FileOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.RecordWriter
import org.apache.hadoop.util.Progressable
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.auron.NativeHelper
import org.apache.spark.sql.auron.NativeRDD
import org.apache.spark.sql.auron.NativeSupports
import org.apache.spark.sql.auron.Shims
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.BasicWriteJobStatsTracker
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable

abstract class NativeParquetInsertIntoHiveTableBase(
    cmd: InsertIntoHiveTable,
    override val child: SparkPlan)
    extends UnaryExecNode
    with NativeSupports {

  override lazy val metrics: Map[String, SQLMetric] = SortedMap[String, SQLMetric]() ++
    BasicWriteJobStatsTracker.metrics ++
    Map(
      NativeHelper
        .getDefaultNativeMetrics(sparkContext)
        .filterKeys(Set("stage_id", "output_rows", "elapsed_compute"))
        .toSeq
        :+ ("io_time", SQLMetrics.createNanoTimingMetric(sparkContext, "Native.io_time"))
        :+ ("bytes_written", SQLMetrics
          .createSizeMetric(sparkContext, "Native.bytes_written")): _*)

  def check(): Unit = {
    val hadoopConf = sparkContext.hadoopConfiguration
    val tblStorage = cmd.table.storage
    val outputFormatClassName = tblStorage.outputFormat.getOrElse("").toLowerCase(Locale.ROOT)
    val encryptEnabled: Boolean = hadoopConf.getBoolean("parquet.encrypt.enable", false)

    assert(outputFormatClassName.endsWith("mapredparquetoutputformat"), "not parquet format")
    assert(!encryptEnabled, "not supported writing encrypted table")
  }
  check()

  @transient
  val wrapped: DataWritingCommandExec = {
    val transformedTable = {
      val tblStorage = cmd.table.storage
      cmd.table.withNewStorage(
        tblStorage.locationUri,
        tblStorage.inputFormat,
        outputFormat = Some(classOf[AuronMapredParquetOutputFormat].getName),
        tblStorage.compressed,
        serde = Some(classOf[ParquetHiveSerDe].getName),
        tblStorage.properties)
    }

    val transformedCmd = getInsertIntoHiveTableCommand(
      transformedTable,
      cmd.partition,
      cmd.query,
      cmd.overwrite,
      cmd.ifPartitionNotExists,
      cmd.outputColumnNames,
      metrics)
    DataWritingCommandExec(transformedCmd, child)
  }

  override def output: Seq[Attribute] = wrapped.output
  override def outputPartitioning: Partitioning = wrapped.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = wrapped.outputOrdering
  override def doExecute(): RDD[InternalRow] = wrapped.execute()

  override def executeCollect(): Array[InternalRow] = wrapped.executeCollect()
  override def executeTake(n: Int): Array[InternalRow] = wrapped.executeTake(n)
  override def executeToIterator(): Iterator[InternalRow] = wrapped.executeToIterator()

  override def doExecuteNative(): NativeRDD = {
    Shims.get.createConvertToNativeExec(wrapped).executeNative()
  }

  override def nodeName: String =
    s"NativeParquetInsert ${cmd.table.identifier.unquotedString}"

  protected def getInsertIntoHiveTableCommand(
      table: CatalogTable,
      partition: Map[String, Option[String]],
      query: LogicalPlan,
      overwrite: Boolean,
      ifPartitionNotExists: Boolean,
      outputColumnNames: Seq[String],
      metrics: Map[String, SQLMetric]): InsertIntoHiveTable
}

// A dummy output format which does not write anything but only pass output path to native ParquetSinkExec.
class AuronMapredParquetOutputFormat
    extends FileOutputFormat[NullWritable, NullWritable]
    with HiveOutputFormat[NullWritable, NullWritable] {

  override def getRecordWriter(
      fileSystem: FileSystem,
      jobConf: JobConf,
      name: String,
      progressable: Progressable): RecordWriter[NullWritable, NullWritable] =
    throw new NotImplementedError()

  override def getHiveRecordWriter(
      jobConf: JobConf,
      finalOutPath: Path,
      valueClass: Class[_ <: Writable],
      isCompressed: Boolean,
      tableProperties: Properties,
      progress: Progressable): FileSinkOperator.RecordWriter = {

    new FileSinkOperator.RecordWriter {
      override def write(w: Writable): Unit = {
        ParquetSinkTaskContext.get.processingOutputFiles.offer(finalOutPath.toString)
      }

      override def close(abort: Boolean): Unit = {}
    }
  }
}

case class OutputFileStat(path: String, numRows: Long, numBytes: Long)

class ParquetSinkTaskContext {
  var isNative: Boolean = false
  val processingOutputFiles = new LinkedBlockingDeque[String]()
  val processedOutputFiles = new util.ArrayDeque[OutputFileStat]()
}

object ParquetSinkTaskContext {
  private val instances = mutable.Map[Long, ParquetSinkTaskContext]()

  def get: ParquetSinkTaskContext = {
    val taskId = TaskContext.get.taskAttemptId()
    instances.getOrElseUpdate(
      taskId, {
        TaskContext.get().addTaskCompletionListener(_ => instances.remove(taskId))
        new ParquetSinkTaskContext
      })
  }

}
