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

package org.apache.spark.sql.execution.blaze.shuffle

import java.io.File
import java.io.FileInputStream
import java.io.FilterInputStream
import java.io.InputStream
import java.lang.reflect.Field

import org.apache.spark.InterruptibleIterator
import org.apache.spark.MapOutputTracker
import org.apache.spark.SparkEnv
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config
import org.apache.spark.io.CompressionCodec
import org.apache.spark.network.util.LimitedInputStream
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.spark.shuffle.ShuffleReader
import org.apache.spark.shuffle.ShuffleReadMetricsReporter
import org.apache.spark.sql.execution.blaze.arrowio.ArrowReaderIterator
import org.apache.spark.sql.execution.blaze.arrowio.IpcInputStreamIterator
import org.apache.spark.storage.BlockId
import org.apache.spark.storage.BlockManager
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.storage.FileSegment
import org.apache.spark.storage.ShuffleBlockFetcherIterator
import org.apache.spark.util.CompletionIterator

class ArrowBlockStoreShuffleReader[K, C](
                                             handle: BaseShuffleHandle[K, _, C],
                                             blocksByAddress: () => Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])],
                                             context: TaskContext,
                                             readMetrics: ShuffleReadMetricsReporter,
                                             serializerManager: SerializerManager = SparkEnv.get.serializerManager,
                                             blockManager: BlockManager = SparkEnv.get.blockManager,
                                             mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker,
                                             shouldBatchFetch: Boolean = false)
  extends ShuffleReader[K, C]
    with Logging {

  private val dep = handle.dependency

  private def fetchIterator: Iterator[(BlockId, InputStream)] = {
    new ShuffleBlockFetcherIterator(
      context,
      SparkEnv.get.blockManager.blockStoreClient,
      SparkEnv.get.blockManager,
      blocksByAddress(),
      (_, inputStream) => inputStream,
      // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
      SparkEnv.get.conf.get(config.REDUCER_MAX_SIZE_IN_FLIGHT) * 1024 * 1024,
      SparkEnv.get.conf.get(config.REDUCER_MAX_REQS_IN_FLIGHT),
      SparkEnv.get.conf.get(config.REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS),
      SparkEnv.get.conf.get(config.MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM),
      SparkEnv.get.conf.get(config.SHUFFLE_DETECT_CORRUPT),
      SparkEnv.get.conf.get(config.SHUFFLE_DETECT_CORRUPT_MEMORY),
      readMetrics,
      fetchContinuousBlocksInBatch).toCompletionIterator
  }

  def readIpc(): Iterator[Object] = { // FileSegment | ReadableByteChannel
    def getFileSegmentFromInputStream(in: InputStream): Option[FileSegment] = {
      object Helper {
        // scalastyle:off classforname
        val bufferReleasingInputStreamClass: Class[_] =
          Class.forName("org.apache.spark.storage.BufferReleasingInputStream")
        // scalastyle:on classforname
        val delegateField: Field = bufferReleasingInputStreamClass.getDeclaredField("delegate")
        val inField: Field = classOf[FilterInputStream].getDeclaredField("in")
        val limitField: Field = classOf[LimitedInputStream].getDeclaredField("left")
        val pathField: Field = classOf[FileInputStream].getDeclaredField("path")
        delegateField.setAccessible(true)
        inField.setAccessible(true)
        limitField.setAccessible(true)
        pathField.setAccessible(true)
      }
      Helper.delegateField.get(in) match {
        case in: LimitedInputStream =>
          val limit = Helper.limitField.getLong(in)
          Helper.inField.get(in) match {
            case in: FileInputStream =>
              val path = Helper.pathField.get(in).asInstanceOf[String]
              val offset = in.getChannel.position()
              val fileSegment = new FileSegment(new File(path), offset, limit)
              Some(fileSegment)
            case _ =>
              None
          }
        case _ =>
          None
      }
    }

    fetchIterator.flatMap {
      case (_, inputStream) =>
        getFileSegmentFromInputStream(inputStream) match {
          case Some(fileSegment) =>
            Iterator.single(fileSegment)
          case None =>
            IpcInputStreamIterator(inputStream, decompressingNeeded = false, context)
        }
    }
  }

  /** Read the combined key-values for this reduce task */
  override def read(): Iterator[Product2[K, C]] = {
    val recordIter = fetchIterator
      .flatMap {
        case (_, inputStream) =>
          IpcInputStreamIterator(inputStream, decompressingNeeded = true, context)
      }
      .flatMap { channel =>
        new ArrowReaderIterator(channel, context).map((0, _)) // use 0 as key since it's not used
      }

    // Update the context task metrics for each record read.
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      recordIter.map { record =>
        readMetrics.incRecordsRead(1)
        record
      },
      context.taskMetrics().mergeShuffleReadMetrics())

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
