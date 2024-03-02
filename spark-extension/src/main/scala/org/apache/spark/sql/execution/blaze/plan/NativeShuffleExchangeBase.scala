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

import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.spark.Partitioner
import org.apache.spark.ShuffleDependency
import org.apache.spark.SparkEnv
import org.apache.spark.TaskContext
import org.blaze.protobuf.{IpcReaderExecNode, PhysicalHashRepartition, PhysicalPlanNode, Schema}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.ShuffleWriteProcessor
import org.apache.spark.sql.blaze.JniBridge
import org.apache.spark.sql.blaze.MetricNode
import org.apache.spark.sql.blaze.NativeConverters
import org.apache.spark.sql.blaze.NativeHelper
import org.apache.spark.sql.blaze.NativeRDD
import org.apache.spark.sql.blaze.NativeSupports
import org.apache.spark.sql.blaze.Shims
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.metric.SQLShuffleReadMetricsReporter
import org.apache.spark.sql.execution.metric.SQLShuffleWriteMetricsReporter
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnsafeRowSerializer
import org.apache.spark.sql.execution.blaze.shuffle.BlazeBlockStoreShuffleReaderBase
import org.apache.spark.sql.execution.blaze.shuffle.BlazeShuffleDependency
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.CompletionIterator
import org.apache.spark.OneToOneDependency

abstract class NativeShuffleExchangeBase(
    override val outputPartitioning: Partitioning,
    override val child: SparkPlan)
    extends ShuffleExchangeLike
    with NativeSupports {

  override val nodeName: String = "NativeShuffleExchange"

  val serializer: UnsafeRowSerializer =
    new UnsafeRowSerializer(child.output.size, longMetric("dataSize"))

  @transient
  lazy val inputRDD: RDD[InternalRow] = if (NativeHelper.isNative(child)) {
    NativeHelper.executeNative(child)
  } else {
    child.execute()
  }

  /**
   * A [[ShuffleDependency]] that will partition rows of its child based on the partitioning
   * scheme defined in `newPartitioning`. Those partitions of the returned ShuffleDependency will
   * be the input of shuffle.
   */
  @transient
  lazy val shuffleDependency: ShuffleDependency[Int, InternalRow, InternalRow] = {
    prepareNativeShuffleDependency(
      inputRDD,
      child.output,
      outputPartitioning,
      serializer,
      metrics)
  }

  def nativeSchema: Schema = Util.getNativeSchema(child.output)

  private def nativeHashExprs = outputPartitioning match {
    case HashPartitioning(expressions, _) =>
      expressions.map(expr => NativeConverters.convertExpr(expr)).toList
    case _ => null
  }

  // check whether native converting is supported
  nativeSchema
  nativeHashExprs

  protected def doExecuteNonNative(): RDD[InternalRow]

  override def doExecuteNative(): NativeRDD = {
    val shuffleHandle = shuffleDependency.shuffleHandle
    val rdd = doExecuteNonNative()

    val nativeMetrics = MetricNode(
      Map(),
      Nil,
      Some({
        case ("output_rows", v) =>
          val shuffleReadMetrics = TaskContext.get.taskMetrics().createTempShuffleReadMetrics()
          new SQLShuffleReadMetricsReporter(shuffleReadMetrics, metrics).incRecordsRead(v)
          TaskContext.get.taskMetrics().mergeShuffleReadMetrics()
        case _ =>
      }))

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      rddPartitions = rdd.partitions,
      rddDependencies = shuffleDependency :: Nil,
      Shims.get.getRDDShuffleReadFull(rdd),
      (partition, taskContext) => {
        val shuffleReadMetrics = taskContext.taskMetrics().createTempShuffleReadMetrics()
        val metricReporter = new SQLShuffleReadMetricsReporter(shuffleReadMetrics, metrics)
        val nativeSchema = this.nativeSchema

        // store fetch iterator in jni resource before native compute
        val jniResourceId = s"NativeShuffleReadExec:${UUID.randomUUID().toString}"
        val reader = SparkEnv.get.shuffleManager
          .getReader(
            shuffleHandle,
            partition.index,
            partition.index + 1,
            taskContext,
            metricReporter)
          .asInstanceOf[BlazeBlockStoreShuffleReaderBase[_, _]]

        val ipcIterator = CompletionIterator[Object, Iterator[Object]](
          reader.readIpc(),
          taskContext.taskMetrics().mergeShuffleReadMetrics())
        JniBridge.resourcesMap.put(jniResourceId, () => ipcIterator)

        PhysicalPlanNode
          .newBuilder()
          .setIpcReader(
            IpcReaderExecNode
              .newBuilder()
              .setSchema(nativeSchema)
              .setNumPartitions(rdd.getNumPartitions)
              .setIpcProviderResourceId(jniResourceId)
              .build())
          .build()
      },
      friendlyName = "NativeRDD.ShuffleRead")
  }

  def createNativeShuffleWriteProcessor(
      metrics: Map[String, SQLMetric],
      numPartitions: Int): ShuffleWriteProcessor

  def prepareNativeShuffleDependency(
      rdd: RDD[InternalRow],
      outputAttributes: Seq[Attribute],
      outputPartitioning: Partitioning,
      serializer: Serializer,
      metrics: Map[String, SQLMetric]): ShuffleDependency[Int, InternalRow, InternalRow] = {

    val nativeInputRDD = rdd.asInstanceOf[NativeRDD]
    val numPartitions = outputPartitioning.numPartitions
    val nativeMetrics = MetricNode(
      Map(),
      nativeInputRDD.metrics :: Nil,
      Some({
        case ("stage_id", v) => metrics("stage_id") += v
        case ("data_size", v) => metrics("dataSize") += v
        case ("output_rows", v) =>
          val shuffleWriteMetrics = TaskContext.get.taskMetrics().shuffleWriteMetrics
          new SQLShuffleWriteMetricsReporter(shuffleWriteMetrics, metrics).incRecordsWritten(v)
        case ("elapsed_compute", v) =>
          val shuffleWriteMetrics = TaskContext.get.taskMetrics().shuffleWriteMetrics
          new SQLShuffleWriteMetricsReporter(shuffleWriteMetrics, metrics).incWriteTime(v)
        case ("mem_spill_count", v) if v > 0 => metrics("mem_spill_count").add(v)
        case ("mem_spill_size", v) if v > 0 => metrics("mem_spill_size").add(v)
        case ("mem_spill_iotime", v) if v > 0 => metrics("mem_spill_iotime").add(v)
        case ("disk_spill_size", v) if v > 0 => metrics("disk_spill_size").add(v)
        case ("disk_spill_iotime", v) if v > 0 => metrics("disk_spill_iotime").add(v)
        case _ =>
      }))
    val nativeHashExprs = this.nativeHashExprs

    val nativeShuffleRDD = new NativeRDD(
      nativeInputRDD.sparkContext,
      nativeMetrics,
      nativeInputRDD.partitions,
      new OneToOneDependency(nativeInputRDD) :: Nil,
      nativeInputRDD.isShuffleReadFull,
      (partition, taskContext) => {
        val nativeInputPartition = nativeInputRDD.partitions(partition.index)
        val nativeOutputPartitioning = outputPartitioning match {
          case SinglePartition =>
            PhysicalHashRepartition
              .newBuilder()
              .setPartitionCount(1)
          case HashPartitioning(_, _) =>
            PhysicalHashRepartition
              .newBuilder()
              .setPartitionCount(numPartitions)
              .addAllHashExpr(nativeHashExprs.asJava)
          case p =>
            throw new NotImplementedError(s"cannot convert partitioning to native: $p")
        }

        val input = nativeInputRDD.nativePlan(nativeInputPartition, taskContext)
        val nativeShuffleWriteExec =
          Shims.get.getShuffleWriteExec(input, nativeOutputPartitioning)
        nativeShuffleWriteExec
      },
      friendlyName = "NativeRDD.ShuffleWrite")

    val dependency = new BlazeShuffleDependency[Int, InternalRow, InternalRow](
      nativeShuffleRDD.map((0, _)),
      serializer = serializer,
      shuffleWriterProcessor = createNativeShuffleWriteProcessor(metrics, numPartitions),
      partitioner = new Partitioner {
        override def numPartitions: Int = outputPartitioning.numPartitions

        override def getPartition(key: Any): Int = key.asInstanceOf[Int]
      },
      schema = StructType.fromAttributes(outputAttributes))
    dependency
  }
}
