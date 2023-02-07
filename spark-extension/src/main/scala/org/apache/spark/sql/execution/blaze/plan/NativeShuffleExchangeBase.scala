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
import org.blaze.protobuf.{
  IpcReadMode,
  IpcReaderExecNode,
  PhysicalExprNode,
  PhysicalHashRepartition,
  PhysicalPlanNode,
  RssShuffleWriterExecNode,
  Schema,
  ShuffleWriterExecNode
}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.shuffle.ShuffleWriteProcessor
import org.apache.spark.sql.blaze.JniBridge
import org.apache.spark.sql.blaze.MetricNode
import org.apache.spark.sql.blaze.NativeConverters
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
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.CompletionIterator

abstract class NativeShuffleExchangeBase(
    override val outputPartitioning: Partitioning,
    override val child: SparkPlan)
    extends ShuffleExchangeLike
    with NativeSupports {

  override val nodeName: String = "NativeShuffleExchange"

  val serializer: UnsafeRowSerializer =
    new UnsafeRowSerializer(child.output.size, longMetric("dataSize"))

  @transient
  lazy val inputRDD: RDD[InternalRow] = child.execute()

  /**
   * A [[ShuffleDependency]] that will partition rows of its child based on
   * the partitioning scheme defined in `newPartitioning`. Those partitions of
   * the returned ShuffleDependency will be the input of shuffle.
   */
  @transient
  lazy val shuffleDependency: ShuffleDependency[Int, InternalRow, InternalRow] = {
    prepareNativeShuffleDependency(
      inputRDD,
      child.output,
      outputPartitioning,
      serializer,
      metrics,
      nativeHashExprs)
  }

  val nativeSchema: Schema = Util.getNativeSchema(child.output)
  val nativeHashExprs: List[PhysicalExprNode] = outputPartitioning match {
    case HashPartitioning(expressions, _) =>
      expressions.map(expr => NativeConverters.convertExpr(expr)).toList
    case _ => null
  }

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
      Shims.get.rddShims.getShuffleReadFull(rdd),
      (partition, taskContext) => {
        val shuffleReadMetrics = taskContext.taskMetrics().createTempShuffleReadMetrics()
        val metricReporter = new SQLShuffleReadMetricsReporter(shuffleReadMetrics, metrics)

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
              .setMode(IpcReadMode.CHANNEL_AND_FILE_SEGMENT)
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
      metrics: Map[String, SQLMetric],
      nativeHashExprs: List[PhysicalExprNode])
      : ShuffleDependency[Int, InternalRow, InternalRow] = {

    val nativeInputRDD = rdd.asInstanceOf[NativeRDD]
    val numPartitions = outputPartitioning.numPartitions
    val nativeMetrics = MetricNode(
      Map(),
      nativeInputRDD.metrics :: Nil,
      Some({
        case ("output_rows", v) =>
          val shuffleWriteMetrics = TaskContext.get.taskMetrics().shuffleWriteMetrics
          new SQLShuffleWriteMetricsReporter(shuffleWriteMetrics, metrics).incRecordsWritten(v)
        case ("compute_elapsed", v) =>
          val shuffleWriteMetrics = TaskContext.get.taskMetrics().shuffleWriteMetrics
          new SQLShuffleWriteMetricsReporter(shuffleWriteMetrics, metrics).incWriteTime(v)
        case ("spilled_bytes", v) => metrics("spilled_bytes").add(v)
        case ("spill_count", v) => metrics("spill_count").add(v)
        case _ =>
      }))

    val nativeShuffleRDD = new NativeRDD(
      nativeInputRDD.sparkContext,
      nativeMetrics,
      nativeInputRDD.partitions,
      nativeInputRDD.dependencies,
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
          Shims.get.shuffleShims.getShuffleWriteExec(input, nativeOutputPartitioning)
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

  override def doCanonicalize(): SparkPlan =
    ShuffleExchangeExec(outputPartitioning, child).canonicalized
}

object NativeShuffleExchangeBase extends Logging {

  /**
   * Determines whether records must be defensively copied before being sent to the shuffle.
   * Several of Spark's shuffle components will buffer deserialized Java objects in memory. The
   * shuffle code assumes that objects are immutable and hence does not perform its own defensive
   * copying. In Spark SQL, however, operators' iterators return the same mutable `Row` object. In
   * order to properly shuffle the output of these operators, we need to perform our own copying
   * prior to sending records to the shuffle. This copying is expensive, so we try to avoid it
   * whenever possible. This method encapsulates the logic for choosing when to copy.
   *
   * In the long run, we might want to push this logic into core's shuffle APIs so that we don't
   * have to rely on knowledge of core internals here in SQL.
   *
   * See SPARK-2967, SPARK-4479, and SPARK-7375 for more discussion of this issue.
   *
   * @param partitioner the partitioner for the shuffle
   * @return true if rows should be copied before being shuffled, false otherwise
   */
  private def needToCopyObjectsBeforeShuffle(partitioner: Partitioner): Boolean = {
    // Note: even though we only use the partitioner's `numPartitions` field, we require it to be
    // passed instead of directly passing the number of partitions in order to guard against
    // corner-cases where a partitioner constructed with `numPartitions` partitions may output
    // fewer partitions (like RangePartitioner, for example).
    val conf = SparkEnv.get.conf
    val shuffleManager = SparkEnv.get.shuffleManager
    val sortBasedShuffleOn = shuffleManager.isInstanceOf[SortShuffleManager]
    val bypassMergeThreshold = conf.getInt("spark.shuffle.sort.bypassMergeThreshold", 200)
    val numParts = partitioner.numPartitions
    if (sortBasedShuffleOn) {
      if (numParts <= bypassMergeThreshold) {
        // If we're using the original SortShuffleManager and the number of output partitions is
        // sufficiently small, then Spark will fall back to the hash-based shuffle write path, which
        // doesn't buffer deserialized records.
        // Note that we'll have to remove this case if we fix SPARK-6026 and remove this bypass.
        false
      } else if (numParts <= SortShuffleManager.MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE) {
        // SPARK-4550 and  SPARK-7081 extended sort-based shuffle to serialize individual records
        // prior to sorting them. This optimization is only applied in cases where shuffle
        // dependency does not specify an aggregator or ordering and the record serializer has
        // certain properties and the number of partitions doesn't exceed the limitation. If this
        // optimization is enabled, we can safely avoid the copy.
        //
        // Exchange never configures its ShuffledRDDs with aggregators or key orderings, and the
        // serializer in Spark SQL always satisfy the properties, so we only need to check whether
        // the number of partitions exceeds the limitation.
        false
      } else {
        // Spark's SortShuffleManager uses `ExternalSorter` to buffer records in memory, so we must
        // copy.
        true
      }
    } else {
      // Catch-all case to safely handle any future ShuffleManager implementations.
      true
    }
  }
  def canUseNativeShuffleWrite(outputPartitioning: Partitioning): Boolean = {
    outputPartitioning.numPartitions == 1 || outputPartitioning.isInstanceOf[HashPartitioning]
  }
}
