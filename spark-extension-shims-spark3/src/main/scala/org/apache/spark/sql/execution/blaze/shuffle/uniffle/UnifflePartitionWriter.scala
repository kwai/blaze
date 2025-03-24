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
package org.apache.spark.sql.execution.blaze.shuffle.uniffle

import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.shuffle.writer.RssShuffleWriter
import org.apache.spark.sql.execution.blaze.shuffle.RssPartitionWriterBase
import org.apache.uniffle.common.ShuffleBlockInfo

import java.nio.ByteBuffer

class UnifflePartitionWriter[K, V, C](
    mapId: Int,
    numPartitions: Int,
    metrics: ShuffleWriteMetricsReporter,
    rssShuffleWriter: RssShuffleWriter[K, V, C])
    extends RssPartitionWriterBase
    with Logging {
  private val mapStatusLengths: Array[Long] = Array.fill(numPartitions)(0L)
  private val rssShuffleWriterPushBlocksMethod = {
    val method = rssShuffleWriter.getClass.getDeclaredMethod(
      "processShuffleBlockInfos",
      classOf[java.util.List[ShuffleBlockInfo]])
    method.setAccessible(true)
    method
  }

  override def write(partitionId: Int, buffer: ByteBuffer): Unit = {
    val bytes = new Array[Byte](buffer.limit())
    buffer.get(bytes)
    val bytesWritten = bytes.length

    val bufferManager = rssShuffleWriter.getBufferManager
    val shuffleBlockInfos = rssShuffleWriter.synchronized {
      bufferManager.addPartitionData(partitionId, bytes)
    }
    if (shuffleBlockInfos != null && !shuffleBlockInfos.isEmpty) {
      rssShuffleWriter.synchronized {
        rssShuffleWriterPushBlocksMethod.invoke(rssShuffleWriter, shuffleBlockInfos)
      }
    }
    metrics.incBytesWritten(bytesWritten)
    mapStatusLengths(partitionId) += bytesWritten
  }

  override def flush(): Unit = {}

  override def close(): Unit = {
    val start = System.currentTimeMillis()
    val bufferManager = rssShuffleWriter.getBufferManager
    val restBlocks = bufferManager.clear()
    if (restBlocks != null && !restBlocks.isEmpty) {
      rssShuffleWriterPushBlocksMethod.invoke(rssShuffleWriter, restBlocks)
    }
    val writtenDurationMs = bufferManager.getWriteTime + (System.currentTimeMillis() - start)
    metrics.incWriteTime(writtenDurationMs)
  }

  override def getPartitionLengthMap: Array[Long] = mapStatusLengths
}
