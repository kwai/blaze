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

import java.nio.file.Paths
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.file.Files
import java.util.Random
import java.util.function.Supplier
import java.util.UUID

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark._
import org.apache.spark.rdd.MapPartitionsRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.shuffle.ShuffleWriteProcessor
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.sql.blaze.JniBridge
import org.apache.spark.sql.blaze.MetricNode
import org.apache.spark.sql.blaze.NativeConverters
import org.apache.spark.sql.blaze.NativeRDD
import org.apache.spark.sql.blaze.NativeSupports
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors.attachTree
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.blaze.plan.ArrowShuffleExchangeExec301.canUseNativeShuffleWrite
import org.apache.spark.sql.execution.blaze.shuffle.ArrowBlockStoreShuffleReader301
import org.apache.spark.sql.execution.blaze.shuffle.ArrowShuffleDependency
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.metric.SQLShuffleReadMetricsReporter
import org.apache.spark.sql.execution.metric.SQLShuffleWriteMetricsReporter
import org.apache.spark.sql.execution.SQLExecution.EXECUTION_ID_KEY
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.MutablePair
import org.apache.spark.util.collection.unsafe.sort.PrefixComparators
import org.apache.spark.util.collection.unsafe.sort.RecordComparator
import org.apache.spark.util.CompletionIterator
import org.blaze.protobuf.IpcReaderExecNode
import org.blaze.protobuf.IpcReadMode
import org.blaze.protobuf.PhysicalHashRepartition
import org.blaze.protobuf.PhysicalPlanNode
import org.blaze.protobuf.Schema
import org.blaze.protobuf.ShuffleWriterExecNode

case class ArrowShuffleExchangeExec301(
    var newPartitioning: Partitioning,
    override val child: SparkPlan)
    extends ShuffleExchangeLike
    with NativeSupports {

  val shuffleDescStr =
    s"$newPartitioning[${child.output.map(_.dataType.simpleString).mkString(",")}]"

  // NOTE: coordinator can be null after serialization/deserialization,
  //       e.g. it can be null on the Executor side
  override lazy val writeMetrics =
    SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext, shuffleDescStr)
  override lazy val readMetrics =
    SQLShuffleReadMetricsReporter.createShuffleReadMetrics(sparkContext, shuffleDescStr)
  override lazy val metrics: Map[String, SQLMetric] =
    Map(
      "dataSize" -> SQLMetrics
        .createSizeMetric(sparkContext, "data size")) ++ readMetrics ++ writeMetrics

  override def nodeName: String = {
    "ArrowShuffleExchange"
  }

  override def outputPartitioning: Partitioning = newPartitioning

  private val serializer: UnsafeRowSerializer =
    new UnsafeRowSerializer(child.output.size, longMetric("dataSize"))

  /**
   * Returns a [[ShuffleDependency]] that will partition rows of its child based on
   * the partitioning scheme defined in `newPartitioning`. Those partitions of
   * the returned ShuffleDependency will be the input of shuffle.
   */
  private def prepareShuffleDependency(): ShuffleDependency[Int, InternalRow, InternalRow] = {
    val rdd: RDD[InternalRow] = child.execute()
    if (canUseNativeShuffleWrite(rdd, newPartitioning)) {
      ArrowShuffleExchangeExec301.prepareNativeShuffleDependency(
        rdd,
        child.output,
        newPartitioning,
        serializer,
        metrics)
    } else {
      ArrowShuffleExchangeExec301.prepareShuffleDependency(
        rdd,
        child.output,
        newPartitioning,
        serializer,
        metrics)
    }
  }

  private def preparePostShuffleRDD(
      shuffleDependency: ShuffleDependency[Int, InternalRow, InternalRow],
      specifiedPartitionStartIndices: Option[Array[Int]] = None): ShuffledRowRDD = {
    // If an array of partition start indices is provided, we need to use this array
    // to create the ShuffledRowRDD. Also, we need to update newPartitioning to
    // update the number of post-shuffle partitions.
    specifiedPartitionStartIndices.foreach { indices =>
      assert(newPartitioning.isInstanceOf[HashPartitioning])
      newPartitioning = UnknownPartitioning(indices.length)
    }
    new ShuffledRowRDD(shuffleDependency, readMetrics, specifiedPartitionStartIndices)
  }

  /**
   * Caches the created ShuffleRowRDD so we can reuse that.
   */
  private var cachedShuffleRDD: ShuffledRowRDD = null

  protected override def doExecute(): RDD[InternalRow] =
    attachTree(this, "execute") {
      // Returns the same ShuffleRowRDD if this plan is used by multiple plans.
      if (cachedShuffleRDD == null) {
        val shuffleDependency = prepareShuffleDependency()
        cachedShuffleRDD = preparePostShuffleRDD(shuffleDependency)
      }
      cachedShuffleRDD
    }

  private var _mapOutputStatistics: MapOutputStatistics = null

  override def mapOutputStatistics: MapOutputStatistics = _mapOutputStatistics

  override def eagerExecute(): RDD[InternalRow] = {
    if (cachedShuffleRDD == null) {
      val shuffleDependency = prepareShuffleDependency()
      shuffleDependency.rdd.stageId = this.stageId
      val allOperatorMetrics = new mutable.HashSet[SQLMetric]()
      metrics.foreach {
        case (_: String, v: SQLMetric) =>
          allOperatorMetrics.add(v)
      }
      transformUp {
        case p: SparkPlan =>
          p.metrics.foreach {
            case (_: String, sqlMetric: SQLMetric) =>
              allOperatorMetrics.add(sqlMetric)
          }
          p
      }
      val executionId = sqlContext.sparkContext.getLocalProperty(EXECUTION_ID_KEY)
      SQLMetrics.postDriverMetricUpdates(
        sqlContext.sparkContext,
        executionId,
        allOperatorMetrics.toSeq)
      if (shuffleDependency.rdd.partitions.length != 0) {
        // submitMapStage does not accept RDD with 0 partition.
        // So, we will not submit this dependency.
        val submittedStageFuture = sqlContext.sparkContext.submitMapStage(shuffleDependency)
        try {
          val stat = submittedStageFuture.get()

          // NOTE:
          //  in the case that one ipc contains little number of records, the data size may
          //  be much more larger than unsafe row shuffle (because of a lot of redundant
          //  arrow headers). so a data size factor is needed here to prevent incorrect
          //  conversion to later SMJ/BHJ.
          //
          // assume compressed ipc size is smaller than unsafe rows only when the number of
          //  records are larger than 5
          //
          val totalShuffleRecordsWritten =
            metrics(SQLShuffleWriteMetricsReporter.SHUFFLE_RECORDS_WRITTEN).value
          val estimatedIpcCount: Int =
            Math.max(child.outputPartitioning.numPartitions * newPartitioning.numPartitions, 1)
          val avgRecordsPerIpc = totalShuffleRecordsWritten / estimatedIpcCount
          val dataSizeFactor = Math.min(Math.max(avgRecordsPerIpc / 5.0, 0.1), 1.0)

          _mapOutputStatistics = new MapOutputStatistics(
            stat.shuffleId,
            stat.bytesByPartitionId.map(n => (n * dataSizeFactor).ceil.toLong),
            stat.recordsByPartitionId)

        } catch {
          case throwable: Throwable =>
            sparkContext.dagScheduler.sendFailedReason(throwable)
            throw throwable
        }
      }
      cachedShuffleRDD = preparePostShuffleRDD(shuffleDependency)
    }
    cachedShuffleRDD
  }

  val nativeSchema: Schema = NativeConverters.convertSchema(
    StructType(output.map(a => StructField(a.toString(), a.dataType, a.nullable, a.metadata))))

  override def doExecuteNative(): NativeRDD = {
    val shuffleDependency = prepareShuffleDependency()
    val shuffleHandle = shuffleDependency.shuffleHandle
    val rdd = doExecute()

    val nativeMetrics = MetricNode(
      Map(),
      Nil,
      Some({
        case ("output_rows", v) =>
          val shuffleReadMetrics = TaskContext.get.taskMetrics().createTempShuffleReadMetrics()
          new SQLShuffleReadMetricsReporter(shuffleReadMetrics, metrics).incRecordsRead(v)
          TaskContext.get.taskMetrics().mergeShuffleReadMetrics(true)
        case _ =>
      }))

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      rdd.partitions,
      rdd.dependencies,
      rdd.shuffleReadFull,
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
          .asInstanceOf[ArrowBlockStoreShuffleReader301[_, _]]
        JniBridge.resourcesMap.put(
          jniResourceId,
          () => {
            CompletionIterator[Object, Iterator[Object]](
              reader.readIpc(),
              taskContext.taskMetrics().mergeShuffleReadMetrics(true))
          })

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

  override def doCanonicalize(): SparkPlan =
    ShuffleExchangeExec(newPartitioning, child).canonicalized
}

object ArrowShuffleExchangeExec301 {

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
  def canUseNativeShuffleWrite(
      rdd: RDD[InternalRow],
      outputPartitioning: Partitioning): Boolean = {
    rdd.isInstanceOf[NativeRDD] && outputPartitioning.isInstanceOf[HashPartitioning]
  }

  def prepareNativeShuffleDependency(
      rdd: RDD[InternalRow],
      outputAttributes: Seq[Attribute],
      outputPartitioning: Partitioning,
      serializer: Serializer,
      metrics: Map[String, SQLMetric]): ShuffleDependency[Int, InternalRow, InternalRow] = {

    val nativeInputRDD = rdd.asInstanceOf[NativeRDD]
    val HashPartitioning(expressions, numPartitions) =
      outputPartitioning.asInstanceOf[HashPartitioning]

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
        case _ =>
      }))

    val nativeShuffleRDD = new NativeRDD(
      nativeInputRDD.sparkContext,
      nativeMetrics,
      nativeInputRDD.partitions,
      nativeInputRDD.dependencies,
      nativeInputRDD.shuffleReadFull,
      (partition, taskContext) => {
        val nativeInputPartition = nativeInputRDD.partitions(partition.index)
        PhysicalPlanNode
          .newBuilder()
          .setShuffleWriter(
            ShuffleWriterExecNode
              .newBuilder()
              .setInput(nativeInputRDD.nativePlan(nativeInputPartition, taskContext))
              .setOutputPartitioning(
                PhysicalHashRepartition
                  .newBuilder()
                  .setPartitionCount(numPartitions)
                  .addAllHashExpr(expressions.map(NativeConverters.convertExpr).asJava)
                  .build())
              .buildPartial()
          ) // shuffleId is not set at the moment, will be set in ShuffleWriteProcessor
          .build()
      },
      friendlyName = "NativeRDD.ShuffleWrite")

    val dependency = new ArrowShuffleDependency[Int, InternalRow, InternalRow](
      nativeShuffleRDD.map((0, _)),
      serializer = serializer,
      shuffleWriterProcessor = createNativeShuffleWriteProcessor(metrics),
      partitioner = new Partitioner {
        override def numPartitions: Int = outputPartitioning.numPartitions
        override def getPartition(key: Any): Int = key.asInstanceOf[Int]
      },
      schema = StructType.fromAttributes(outputAttributes))
    dependency
  }

  /**
   * Returns a [[ShuffleDependency]] that will partition rows of its child based on
   * the partitioning scheme defined in `newPartitioning`. Those partitions of
   * the returned ShuffleDependency will be the input of shuffle.
   */
  def prepareShuffleDependency(
      rdd: RDD[InternalRow],
      outputAttributes: Seq[Attribute],
      newPartitioning: Partitioning,
      serializer: Serializer,
      writeMetrics: Map[String, SQLMetric]): ShuffleDependency[Int, InternalRow, InternalRow] = {
    val part: Partitioner = newPartitioning match {
      case RoundRobinPartitioning(numPartitions) => new HashPartitioner(numPartitions)
      case HashPartitioning(_, n) =>
        new Partitioner {
          override def numPartitions: Int = n

          // For HashPartitioning, the partitioning key is already a valid partition ID, as we use
          // `HashPartitioning.partitionIdExpression` to produce partitioning key.
          override def getPartition(key: Any): Int = key.asInstanceOf[Int]
        }
      case RangePartitioning(sortingExpressions, numPartitions) =>
        // Extract only fields used for sorting to avoid collecting large fields that does not
        // affect sorting result when deciding partition bounds in RangePartitioner
        val rddForSampling = rdd.mapPartitionsInternal { iter =>
          val projection =
            UnsafeProjection.create(sortingExpressions.map(_.child), outputAttributes)
          val mutablePair = new MutablePair[InternalRow, Null]()
          // Internally, RangePartitioner runs a job on the RDD that samples keys to compute
          // partition bounds. To get accurate samples, we need to copy the mutable keys.
          iter.map(row => mutablePair.update(projection(row).copy(), null))
        }
        // Construct ordering on extracted sort key.
        val orderingAttributes = sortingExpressions.zipWithIndex.map {
          case (ord, i) =>
            ord.copy(child = BoundReference(i, ord.dataType, ord.nullable))
        }
        implicit val ordering = new LazilyGeneratedOrdering(orderingAttributes)
        new RangePartitioner(
          numPartitions,
          rddForSampling,
          ascending = true,
          samplePointsPerPartitionHint = SQLConf.get.rangeExchangeSampleSizePerPartition)
      case SinglePartition =>
        new Partitioner {
          override def numPartitions: Int = 1

          override def getPartition(key: Any): Int = 0
        }
      case _ => sys.error(s"Exchange not implemented for $newPartitioning")
      // TODO: Handle BroadcastPartitioning.
    }

    def getPartitionKeyExtractor(): InternalRow => Any =
      newPartitioning match {
        case RoundRobinPartitioning(numPartitions) =>
          // Distributes elements evenly across output partitions, starting from a random partition.
          var position = new Random(TaskContext.get().partitionId()).nextInt(numPartitions)
          (row: InternalRow) => {
            // The HashPartitioner will handle the `mod` by the number of partitions
            position += 1
            position
          }
        case h: HashPartitioning =>
          val projection =
            UnsafeProjection.create(h.partitionIdExpression :: Nil, outputAttributes)
          row => projection(row).getInt(0)
        case RangePartitioning(sortingExpressions, _) =>
          val projection =
            UnsafeProjection.create(sortingExpressions.map(_.child), outputAttributes)
          row => projection(row)
        case SinglePartition => identity
        case _ => sys.error(s"Exchange not implemented for $newPartitioning")
      }

    val isRoundRobin = newPartitioning.isInstanceOf[RoundRobinPartitioning] &&
      newPartitioning.numPartitions > 1

    val rddWithPartitionIds: RDD[Product2[Int, InternalRow]] = {
      // [SPARK-23207] Have to make sure the generated RoundRobinPartitioning is deterministic,
      // otherwise a retry task may output different rows and thus lead to data loss.
      //
      // Currently we following the most straight-forward way that perform a local sort before
      // partitioning.
      //
      // Note that we don't perform local sort if the new partitioning has only 1 partition, under
      // that case all output rows go to the same partition.
      val newRdd = if (isRoundRobin && SQLConf.get.sortBeforeRepartition) {
        rdd.mapPartitionsInternal { iter =>
          val recordComparatorSupplier = new Supplier[RecordComparator] {
            override def get: RecordComparator = new RecordBinaryComparator()
          }
          // The comparator for comparing row hashcode, which should always be Integer.
          val prefixComparator = PrefixComparators.LONG
          val canUseRadixSort = SQLConf.get.enableRadixSort
          // The prefix computer generates row hashcode as the prefix, so we may decrease the
          // probability that the prefixes are equal when input rows choose column values from a
          // limited range.
          val prefixComputer = new UnsafeExternalRowSorter.PrefixComputer {
            private val result = new UnsafeExternalRowSorter.PrefixComputer.Prefix
            override def computePrefix(
                row: InternalRow): UnsafeExternalRowSorter.PrefixComputer.Prefix = {
              // The hashcode generated from the binary form of a [[UnsafeRow]] should not be null.
              result.isNull = false
              result.value = row.hashCode()
              result
            }
          }
          val pageSize = SparkEnv.get.memoryManager.pageSizeBytes

          val sorter = UnsafeExternalRowSorter.createWithRecordComparator(
            StructType.fromAttributes(outputAttributes),
            recordComparatorSupplier,
            prefixComparator,
            prefixComputer,
            pageSize,
            canUseRadixSort)
          sorter.sort(iter.asInstanceOf[Iterator[UnsafeRow]])
        }
      } else {
        rdd
      }

      // round-robin function is order sensitive if we don't sort the input.
      val isOrderSensitive = isRoundRobin && !SQLConf.get.sortBeforeRepartition
      if (needToCopyObjectsBeforeShuffle(part)) {
        newRdd.mapPartitionsWithIndexInternal(
          (_, iter) => {
            val getPartitionKey = getPartitionKeyExtractor()
            iter.map { row => (part.getPartition(getPartitionKey(row)), row.copy()) }
          },
          isOrderSensitive = isOrderSensitive)
      } else {
        newRdd.mapPartitionsWithIndexInternal(
          (_, iter) => {
            val getPartitionKey = getPartitionKeyExtractor()
            val mutablePair = new MutablePair[Int, InternalRow]()
            iter.map { row => mutablePair.update(part.getPartition(getPartitionKey(row)), row) }
          },
          isOrderSensitive = isOrderSensitive)
      }
    }

    // Now, we manually create a ShuffleDependency. Because pairs in rddWithPartitionIds
    // are in the form of (partitionId, row) and every partitionId is in the expected range
    // [0, part.numPartitions - 1]. The partitioner of this is a PartitionIdPassthrough.
    val dependency =
      new ArrowShuffleDependency[Int, InternalRow, InternalRow](
        rddWithPartitionIds,
        new PartitionIdPassthrough(part.numPartitions),
        serializer,
        shuffleWriterProcessor = createShuffleWriteProcessor(writeMetrics),
        schema = StructType.fromAttributes(outputAttributes))

    dependency
  }

  def createNativeShuffleWriteProcessor(
      metrics: Map[String, SQLMetric]): ShuffleWriteProcessor = {
    new ShuffleWriteProcessor {
      override protected def createMetricsReporter(
          context: TaskContext): ShuffleWriteMetricsReporter = {
        new SQLShuffleWriteMetricsReporter(context.taskMetrics().shuffleWriteMetrics, metrics)
      }

      override def write(
          rdd: RDD[_],
          dep: ShuffleDependency[_, _, _],
          mapId: Int,
          context: TaskContext,
          partition: Partition): MapStatus = {

        val metricsReporter = createMetricsReporter(context)
        val shuffleBlockResolver =
          SparkEnv.get.shuffleManager.shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver]
        val dataFile = shuffleBlockResolver.getDataFile(dep.shuffleId, mapId)
        val tempDataFilename = dataFile.getPath.replace(".data", ".data.tmp")
        val tempIndexFilename = dataFile.getPath.replace(".data", ".index.tmp")
        val tempDataFilePath = Paths.get(tempDataFilename)
        val tempIndexFilePath = Paths.get(tempIndexFilename)

        val nativeShuffleRDD =
          rdd.asInstanceOf[MapPartitionsRDD[_, _]].prev.asInstanceOf[NativeRDD]
        val nativeShuffleWriterExec = PhysicalPlanNode
          .newBuilder()
          .setShuffleWriter(
            ShuffleWriterExecNode
              .newBuilder(nativeShuffleRDD.nativePlan(partition, context).getShuffleWriter)
              .setOutputDataFile(tempDataFilename)
              .setOutputIndexFile(tempIndexFilename)
              .build())
          .build()
        val iterator = NativeSupports.executeNativePlan(
          nativeShuffleWriterExec,
          nativeShuffleRDD.metrics,
          partition,
          context)
        assert(iterator.toArray.isEmpty)

        // get partition lengths from shuffle write output index file
        var offset = 0L
        val partitionLengths = Files
          .readAllBytes(tempIndexFilePath)
          .grouped(8)
          .drop(1) // first partition offset is always 0
          .map(indexBytes => {
            val partitionOffset =
              ByteBuffer.wrap(indexBytes).order(ByteOrder.LITTLE_ENDIAN).getLong
            val partitionLength = partitionOffset - offset
            offset = partitionOffset
            partitionLength
          })
          .toArray

        // update metrics
        val dataSize = Files.size(tempDataFilePath)
        metricsReporter.incBytesWritten(dataSize)

        // commit
        shuffleBlockResolver.writeIndexFileAndCommit(
          dep.shuffleId,
          mapId,
          partitionLengths,
          tempDataFilePath.toFile)
        MapStatus.apply(SparkEnv.get.blockManager.shuffleServerId, partitionLengths)
      }
    }
  }

  /**
   * Create a customized [[ShuffleWriteProcessor]] for SQL which wrap the default metrics reporter
   * with [[SQLShuffleWriteMetricsReporter]] as new reporter for [[ShuffleWriteProcessor]].
   */
  def createShuffleWriteProcessor(metrics: Map[String, SQLMetric]): ShuffleWriteProcessor = {
    new ShuffleWriteProcessor {
      override protected def createMetricsReporter(
          context: TaskContext): ShuffleWriteMetricsReporter = {
        new SQLShuffleWriteMetricsReporter(context.taskMetrics().shuffleWriteMetrics, metrics)
      }
    }
  }
}
