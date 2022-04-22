package org.apache.spark.sql.blaze.execution

import java.nio.channels.SeekableByteChannel

import org.apache.spark.InterruptibleIterator
import org.apache.spark.MapOutputTracker
import org.apache.spark.SparkEnv
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config
import org.apache.spark.io.CompressionCodec
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.spark.shuffle.ShuffleReadMetricsReporter
import org.apache.spark.shuffle.ShuffleReader
import org.apache.spark.sql.blaze.JniBridge
import org.apache.spark.sql.blaze.NativeRDD
import org.apache.spark.storage.BlockId
import org.apache.spark.storage.BlockManager
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.storage.ShuffleBlockFetcherIterator301
import org.apache.spark.util.CompletionIterator

class ArrowBlockStoreShuffleReader301[K, C](
    handle: BaseShuffleHandle[K, _, C],
    blocksByAddress: Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])],
    context: TaskContext,
    readMetrics: ShuffleReadMetricsReporter,
    serializerManager: SerializerManager = SparkEnv.get.serializerManager,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker,
    shouldBatchFetch: Boolean = false)
    extends ShuffleReader[K, C]
    with Logging {

  private val dep = handle.dependency
  private val serializerInstance = dep.serializer.newInstance()

  /** Read the combined key-values for this reduce task */
  override def read(): Iterator[Product2[K, C]] = {
    val buffers = new ShuffleBlockFetcherIterator301(
      context,
      blockManager.blockStoreClient,
      blockManager,
      blocksByAddress,
      // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
      SparkEnv.get.conf.get(config.REDUCER_MAX_SIZE_IN_FLIGHT) * 1024 * 1024,
      SparkEnv.get.conf.get(config.REDUCER_MAX_REQS_IN_FLIGHT),
      SparkEnv.get.conf.get(config.REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS),
      SparkEnv.get.conf.get(config.MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM),
      SparkEnv.get.conf.get(config.SHUFFLE_DETECT_CORRUPT),
      SparkEnv.get.conf.get(config.SHUFFLE_DETECT_CORRUPT_MEMORY),
      readMetrics,
      fetchContinuousBlocksInBatch).toCompletionIterator

    // Store buffers in JniBridge
    JniBridge.resourcesMap.put(
      NativeRDD.getNativeShuffleId(context, handle.shuffleId),
      new InterruptibleIterator(context, buffers.flatMap {
        case (blockId, managedBuffer) =>
          Converters.readManagedBufferToSegmentByteChannels(managedBuffer).toIterator
      }))

    // Create a key/value iterator for each stream
    val recordIter = buffers.flatMap {
      case (blockId, managedBuffer) =>
        // Note: the asKeyValueIterator below wraps a key/value iterator inside of a
        // NextIterator. The NextIterator makes sure that close() is called on the
        // underlying InputStream when all records have been read.

        // use 0 as key since it's not used
        Converters.readManagedBuffer(managedBuffer, context).map(x => (0, x))
    }

    // Update the context task metrics for each record read.
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](recordIter.map {
      record =>
        readMetrics.incRecordsRead(1)
        record
    }, context.taskMetrics().mergeShuffleReadMetrics())

    // An interruptible iterator must be used here in order to support task cancellation
    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)

    val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
      throw new UnsupportedOperationException("aggregate not allowed")
    } else {
      interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
    }

    // Sort the output if there is a sort ordering defined.
    val resultIter = dep.keyOrdering match {
      case Some(keyOrd: Ordering[K]) =>
        throw new UnsupportedOperationException("order not allowed")
      case None =>
        aggregatedIter
    }

    resultIter match {
      case _: InterruptibleIterator[Product2[K, C]] => resultIter
      case _ =>
        // Use another interruptible iterator here to support task cancellation as aggregator
        // or(and) sorter may have consumed previous interruptible iterator.
        new InterruptibleIterator[Product2[K, C]](context, resultIter)
    }
  }

  private def fetchContinuousBlocksInBatch: Boolean = {
    val conf = SparkEnv.get.conf
    val serializerRelocatable = dep.serializer.supportsRelocationOfSerializedObjects
    val compressed = conf.get(config.SHUFFLE_COMPRESS)
    val codecConcatenation = if (compressed) {
      CompressionCodec.supportsConcatenationOfSerializedStreams(
        CompressionCodec.createCodec(conf))
    } else {
      true
    }
    val useOldFetchProtocol = conf.get(config.SHUFFLE_USE_OLD_FETCH_PROTOCOL)

    val doBatchFetch = shouldBatchFetch && serializerRelocatable &&
      (!compressed || codecConcatenation) && !useOldFetchProtocol
    if (shouldBatchFetch && !doBatchFetch) {
      logDebug(
        "The feature tag of continuous shuffle block fetching is set to true, but " +
          "we can not enable the feature because other conditions are not satisfied. " +
          s"Shuffle compress: $compressed, serializer relocatable: $serializerRelocatable, " +
          s"codec concatenation: $codecConcatenation, use old shuffle fetch protocol: " +
          s"$useOldFetchProtocol.")
    }
    doBatchFetch
  }
}
