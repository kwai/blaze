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
import java.util.Locale
import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe
import org.apache.hadoop.hive.ql.io.parquet.write.ParquetRecordWriterWrapper
import org.apache.hadoop.hive.serde2.io.ParquetHiveRecord
import org.apache.hadoop.hive.serde2.SerDeStats
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.Reporter
import org.apache.hadoop.util.Progressable
import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.blaze.protobuf.ParquetProp
import org.blaze.protobuf.ParquetSinkExecNode
import org.blaze.protobuf.PhysicalPlanNode

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.blaze.JniBridge
import org.apache.spark.sql.blaze.MetricNode
import org.apache.spark.sql.blaze.NativeHelper
import org.apache.spark.sql.blaze.NativeRDD
import org.apache.spark.sql.blaze.NativeSupports
import org.apache.spark.sql.blaze.Shims
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.blaze.plan.Helper.InputPlanInfo
import org.apache.spark.sql.execution.blaze.plan.Helper.getTaskResourceId
import org.apache.spark.sql.execution.datasources.BasicWriteJobStatsTracker
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable
import org.apache.spark.util.SerializableConfiguration

abstract class NativeParquetInsertIntoHiveTableBase(
    @transient cmd: InsertIntoHiveTable,
    override val child: SparkPlan)
    extends UnaryExecNode
    with NativeSupports {

  override lazy val metrics: Map[String, SQLMetric] = mutable
    .LinkedHashMap(
      NativeHelper
        .getDefaultNativeMetrics(sparkContext)
        .filterKeys(Set("output_rows", "elapsed_compute"))
        .toSeq
        :+ ("io_time", SQLMetrics.createNanoTimingMetric(sparkContext, "Native.io_time"))
        :+ ("bytes_written", SQLMetrics
          .createSizeMetric(sparkContext, "Native.bytes_written")): _*)
    .toMap

  def check(): Unit = {
    val hadoopConf = sparkContext.hadoopConfiguration
    val tblStorage = cmd.table.storage
    assert(!cmd.partition.values.exists(_.isEmpty), "dynamic partitions not supported")

    val outputFormatClassName = tblStorage.outputFormat.getOrElse("").toLowerCase(Locale.ROOT)
    assert(outputFormatClassName.endsWith("mapredparquetoutputformat"), "not parquet format")

    val encryptEnabled: Boolean = hadoopConf.getBoolean("parquet.encrypt.enable", false)
    assert(!encryptEnabled, "not supported writting encrypted table")

  }
  check()

  @transient
  val wrapped: DataWritingCommandExec = {
    val transformedTable = {
      val tblStorage = cmd.table.storage
      cmd.table.withNewStorage(
        tblStorage.locationUri,
        tblStorage.inputFormat,
        outputFormat = Some(classOf[BlazeMapredParquetOutputFormat].getName),
        tblStorage.compressed,
        serde = Some(classOf[BlazeParquetHiveSerDe].getName),
        tblStorage.properties)
    }
    val transformedCmd = new BlazeInsertIntoHiveTable(
      transformedTable,
      cmd.partition,
      cmd.query,
      cmd.overwrite,
      cmd.ifPartitionNotExists,
      cmd.outputColumnNames)
    DataWritingCommandExec(transformedCmd, PreSinkExec(child, metrics))
  }

  override def output: Seq[Attribute] = wrapped.output
  override def outputPartitioning: Partitioning = wrapped.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = wrapped.outputOrdering
  override def doExecute(): RDD[InternalRow] = wrapped.execute()

  override def executeCollect(): Array[InternalRow] = wrapped.executeCollect()
  override def executeTake(n: Int): Array[InternalRow] = wrapped.executeTake(n)
  override def executeToIterator(): Iterator[InternalRow] = wrapped.executeToIterator()

  override def doExecuteNative(): NativeRDD = {
    throw new RuntimeException("DataWritingCommandExec.doExecuteNative should not be called")
  }

  override def nodeName: String =
    s"NativeParquetInsert ${cmd.table.identifier.unquotedString}"
}

case class PreSinkExec(
    override val child: SparkPlan,
    override val metrics: Map[String, SQLMetric])
    extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def doExecute(): RDD[InternalRow] = {
    val inputRDD = NativeHelper.executeNative(child)
    new RDD[InternalRow](inputRDD.sparkContext, inputRDD.dependencies) {
      override protected def getPartitions: Array[Partition] = inputRDD.partitions
      override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
        // PreSink saves child plan to resource and returns an empty result
        // the plan is later extracted and executed in BlazeParquetRecordWriter.close()
        val inputPlanResourceId = Helper.getTaskResourceId("inputPlan")
        val inputPartition = inputRDD.partitions(split.index)
        val inputPlan = inputRDD.nativePlan(inputPartition, context)
        val inputPlanInfo = InputPlanInfo(inputPlan, inputRDD.metrics, metrics, split, context)
        JniBridge.resourcesMap.put(inputPlanResourceId, inputPlanInfo)
        Iterator.single(InternalRow())
      }
    }
  }

  protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = copy(child = newChild)
}

// extend InsertIntoHiveTable with customized StatsTracker
class BlazeInsertIntoHiveTable(
    table: CatalogTable,
    partition: Map[String, Option[String]],
    query: LogicalPlan,
    overwrite: Boolean,
    ifPartitionNotExists: Boolean,
    outputColumnNames: Seq[String])
    extends InsertIntoHiveTable(
      table,
      partition,
      query,
      overwrite,
      ifPartitionNotExists,
      outputColumnNames) {
  override def basicWriteJobStatsTracker(hadoopConf: Configuration): BasicWriteJobStatsTracker = {
    val serializableHadoopConf = new SerializableConfiguration(hadoopConf)
    Shims.get.createBasicWriteJobStatsTrackerForNativeParquetSink(serializableHadoopConf, metrics)
  }
}

class BlazeMapredParquetOutputFormat extends MapredParquetOutputFormat {
  override def getParquerRecordWriterWrapper(
      realOutputFormat: ParquetOutputFormat[ParquetHiveRecord],
      job: JobConf,
      finalOutputPath: String,
      progress: Progressable,
      tableProperties: Properties): ParquetRecordWriterWrapper = {
    new BlazeParquetRecordWriter(
      realOutputFormat,
      job,
      finalOutputPath,
      progress,
      tableProperties)
  }
}

// a dummy serde class which does nothing but returns null value
class BlazeParquetHiveSerDe extends ParquetHiveSerDe {
  override def initialize(
      configuration: Configuration,
      tableProperties: Properties,
      partitionProperties: Properties): Unit = {}
  override def deserialize(blob: Writable): AnyRef = null
  override def getObjectInspector: ObjectInspector =
    ObjectInspectorFactory.getStandardStructObjectInspector(Nil.asJava, Nil.asJava)

  override def getSerializedClass: Class[_ <: Writable] = classOf[NullWritable]
  override def serialize(obj: Any, objInspector: ObjectInspector): Writable = null
  override def getSerDeStats: SerDeStats = null

}

class BlazeParquetRecordWriter(
    realOutputFormat: ParquetOutputFormat[ParquetHiveRecord],
    job: JobConf,
    outputPath: String,
    progress: Progressable,
    tableProperties: Properties)
    extends ParquetRecordWriterWrapper(
      realOutputFormat,
      job,
      outputPath,
      progress,
      tableProperties) {

  override def write(key: NullWritable, value: ParquetHiveRecord): Unit = {
    // nothing to write
  }

  override def close(reporter: Reporter): Unit = {
    val inputPlanResourceId = Helper.getTaskResourceId("inputPlan")
    val inputPlanInfo = JniBridge.getResource(inputPlanResourceId).asInstanceOf[InputPlanInfo]
    val outputMetrics = TaskContext.get().taskMetrics().outputMetrics

    // init hadoop fs
    val fsResourceId = Helper.getTaskResourceId("fs")
    JniBridge.resourcesMap.put(
      fsResourceId,
      (location: String) => {
        NativeHelper.currentUser.doAs(new PrivilegedExceptionAction[FileSystem] {
          override def run(): FileSystem = FileSystem.get(new URI(location), job)
        })
      })

    val props = job.asScala
      .filter(_.getKey.startsWith("parquet."))
      .map(entry => {
        ParquetProp
          .newBuilder()
          .setKey(entry.getKey)
          .setValue(entry.getValue)
          .build()
      })
      .asJava

    val parquetSink = ParquetSinkExecNode
      .newBuilder()
      .setInput(inputPlanInfo.inputPlan)
      .setPath(outputPath)
      .addAllProp(props)
      .setFsResourceId(fsResourceId)
    val plan = PhysicalPlanNode.newBuilder().setParquetSink(parquetSink).build()
    val executed = NativeHelper.executeNativePlan(
      plan,
      MetricNode(
        inputPlanInfo.metrics,
        inputPlanInfo.inputMetricNode :: Nil,
        Some({
          case ("bytes_written", v) => outputMetrics.setBytesWritten(v)
          case ("output_rows", v) => outputMetrics.setRecordsWritten(v)
          case _ =>
        })),
      inputPlanInfo.partition,
      Some(TaskContext.get))

    assert(executed.isEmpty) // native parquet sink always outputs no records

    // collect WriteTaskStats
    val taskStats = Shims.get.createBasicWriteTaskStats(
      Map(
        "numPartitions" -> 1,
        "numFiles" -> 1,
        "numBytes" -> outputMetrics.bytesWritten,
        "numRows" -> outputMetrics.recordsWritten))
    JniBridge.resourcesMap.put(getTaskResourceId("taskStats"), taskStats)
  }
}

object Helper {
  case class InputPlanInfo(
      inputPlan: PhysicalPlanNode,
      inputMetricNode: MetricNode,
      metrics: Map[String, SQLMetric],
      partition: Partition,
      taskContext: TaskContext)

  def getTaskResourceId(name: String): String = {
    val taskContext = TaskContext.get()
    val stageId = taskContext.stageId()
    val stageAttemptNumber = taskContext.stageAttemptNumber()
    val partitionId = taskContext.partitionId()
    val taskAttemptId = taskContext.taskAttemptId()
    s"ParquetSink:$name:$stageId:$stageAttemptNumber:$partitionId:$taskAttemptId"
  }
}
