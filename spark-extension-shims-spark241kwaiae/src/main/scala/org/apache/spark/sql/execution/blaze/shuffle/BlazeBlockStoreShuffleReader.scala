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

import java.io.InputStream

import org.apache.spark.MapOutputTracker
import org.apache.spark.SparkEnv
import org.apache.spark.TaskContext

import org.apache.spark.internal.Logging
import org.apache.spark.internal.config
import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.spark.shuffle.ShuffleReadMetricsReporter
import org.apache.spark.storage.BlockId
import org.apache.spark.storage.BlockManager
import org.apache.spark.storage.ShuffleBlockFetcherIterator

class BlazeBlockStoreShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    readMetrics: ShuffleReadMetricsReporter,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker,
    startMapId: Option[Int] = None,
    endMapId: Option[Int] = None,
    isLocalShuffleReader: Boolean = false)
    extends BlazeBlockStoreShuffleReaderBase[K, C](handle, context)
    with Logging {

  override def readBlocks(): Iterator[InputStream] = {
    val start = System.currentTimeMillis()
    val blocksByAddress = (startMapId, endMapId) match {
      case (Some(startId), Some(endId)) =>
        mapOutputTracker.getMapSizesByExecutorId(
          handle.shuffleId,
          startPartition,
          endPartition,
          startId,
          endId,
          dep.serializer.supportsRelocationOfSerializedObjects)
      case (None, None) =>
        mapOutputTracker.getMapSizesByExecutorId(
          handle.shuffleId,
          startPartition,
          endPartition,
          dep.serializer.supportsRelocationOfSerializedObjects)
      case (_, _) =>
        throw new IllegalArgumentException("startMapId and endMapId should be both set or unset")
    }
    val end = System.currentTimeMillis()
    context.taskMetrics().executionPhaseMetric.incProduceFetchRequestTime(end - start)

    val blockStreams = new ShuffleBlockFetcherIterator(
      context,
      blockManager.shuffleClient,
      blockManager,
      blocksByAddress,
      streamWrapper = (_, in) => in,
      // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
      SparkEnv.get.conf.getSizeAsMb("spark.reducer.maxSizeInFlight", "48m") * 1024 * 1024,
      SparkEnv.get.conf.getInt("spark.reducer.maxReqsInFlight", Int.MaxValue),
      SparkEnv.get.conf.get(config.REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS),
      SparkEnv.get.conf.get(config.MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM),
      SparkEnv.get.conf.getBoolean("spark.shuffle.detectCorrupt", true),
      readMetrics,
      localShuffle = isLocalShuffleReader &&
        SparkEnv.get.conf.getBoolean("spark.kwai.localShuffle.readHdfs.enabled", false))

    blockStreams.map(_._2)
  }
}
