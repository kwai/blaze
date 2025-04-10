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

import java.nio.ByteBuffer

import org.apache.commons.lang3.reflect.FieldUtils
import org.apache.spark.SparkEnv
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.shuffle.stream.StreamWriterWrapper

import com.kuaishou.dataarch.shuffle.common.io.UnifiedDataInputBuffer
import com.shade.org.apache.commons.lang3.reflect.MethodUtils

class RssPartitionWriter(
    handle: ShuffleHandle,
    mapId: Int,
    metrics: ShuffleWriteMetricsReporter,
    numPartitions: Int)
    extends RssPartitionWriterBase
    with Logging {

  private val tc = TaskContext.get()
  private val streamWriter = SparkEnv.get.shuffleManager
    .asInstanceOf[BlazeRssShuffleManager]
    .getEffectiveManager(handle)
    .getWriter(handle, mapId, tc, metrics)
  private val realWriter = FieldUtils
    .readField(streamWriter, "realWriter", true)
    .asInstanceOf[StreamWriterWrapper[_, _]]
  private val emptyKey = FieldUtils
    .readField(streamWriter, "emptyKey", true)
    .asInstanceOf[UnifiedDataInputBuffer]
  private val partitionLengthMap = new Array[Long](numPartitions)

  tc.addTaskCompletionListener(_ => this.close(true))
  tc.addTaskFailureListener((_, _) => this.close(false))

  override def write(partitionId: Int, buffer: ByteBuffer): Unit = {
    val length = buffer.remaining()
    val valueBuffer = FieldUtils
      .readField(streamWriter, "value", true)
      .asInstanceOf[UnifiedDataInputBuffer]

    val bufferLen = buffer.limit() - buffer.position()
    val bytes = new Array[Byte](bufferLen)
    buffer.get(bytes)
    valueBuffer.reset(bytes, length)

    MethodUtils.invokeMethod(
      realWriter,
      true,
      "append",
      emptyKey,
      valueBuffer,
      Int.box(partitionId),
      Int.box(0))

    partitionLengthMap(partitionId) = length
    metrics.incBytesWritten(length)
  }

  override def flush(): Unit = {
    realWriter.spill()
  }

  override def close(success: Boolean): Unit = realWriter.close()

  override def getPartitionLengthMap: Array[Long] = partitionLengthMap
}
