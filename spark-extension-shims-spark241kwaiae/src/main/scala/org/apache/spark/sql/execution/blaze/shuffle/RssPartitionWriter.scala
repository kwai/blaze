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

import com.kuaishou.dataarch.shuffle.common.io.UnifiedDataInputBuffer
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.stream.{StreamShuffleManager, StreamShuffleWriter, StreamWriterWrapper}
import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.shuffle.{ShuffleHandle, ShuffleWriteMetricsReporter}

import java.nio.ByteBuffer

class RssPartitionWriter(
    handle: ShuffleHandle,
    mapId: Int,
    metrics: ShuffleWriteMetricsReporter,
    numPartitions: Int)
    extends RssPartitionWriterBase
    with Logging {

  val streamWriter = SparkEnv.get.shuffleManager
    .asInstanceOf[BlazeRssShuffleManager]
    .getEffectiveManager(handle)
    .asInstanceOf[StreamShuffleManager]
    .getWriter(handle, mapId, TaskContext.get(), metrics)
    .asInstanceOf[StreamShuffleWriter[_, _]]
  var partitionLengthMap = new Array[Long](numPartitions)

  TaskContext.get.addTaskCompletionListener(_ => this.close())
  TaskContext.get.addTaskFailureListener((_, _) => this.close())

  override def write(partitionId: Int, buffer: ByteBuffer): Unit = {
    val length = buffer.remaining()
    val valueBuffer = classOf[StreamShuffleWriter[_, _]]
      .getDeclaredField("value")
    valueBuffer.setAccessible(true)
    val valueBufferUse = valueBuffer.get(streamWriter).asInstanceOf[UnifiedDataInputBuffer]

    val bufferLen = buffer.limit() - buffer.position()
    val bytes = new Array[Byte](bufferLen)
    buffer.get(bytes)

    valueBufferUse.reset(bytes, length)

    val emptyKey = classOf[StreamShuffleWriter[_, _]]
      .getDeclaredField("emptyKey")
    emptyKey.setAccessible(true)
    val emptyKeyUse = emptyKey.get(streamWriter).asInstanceOf[UnifiedDataInputBuffer]

    val realWriter = classOf[StreamShuffleWriter[_, _]]
      .getDeclaredField("realWriter")
    realWriter.setAccessible(true)
    val realWriterUse = realWriter.get(streamWriter).asInstanceOf[StreamWriterWrapper[_, _]]

    val appendMethod = classOf[StreamWriterWrapper[_, _]]
      .getDeclaredMethod(
        "append",
        classOf[UnifiedDataInputBuffer],
        classOf[UnifiedDataInputBuffer],
        Integer.TYPE,
        Integer.TYPE)
    appendMethod.setAccessible(true)
    appendMethod.invoke(
      realWriterUse,
      emptyKeyUse,
      valueBufferUse,
      new Integer(partitionId),
      new Integer(0))

    partitionLengthMap(partitionId) = length
    metrics.incBytesWritten(length)
  }

  override def flush(): Unit = {
    val realWriter = classOf[StreamShuffleWriter[_, _]]
      .getDeclaredField("realWriter")
    realWriter.setAccessible(true)
    realWriter.get(streamWriter).asInstanceOf[StreamWriterWrapper[_, _]].spill()
  }

  override def close(): Unit = {
    val realWriter = classOf[StreamShuffleWriter[_, _]]
      .getDeclaredField("realWriter")
    realWriter.setAccessible(true)
    realWriter.get(streamWriter).asInstanceOf[StreamWriterWrapper[_, _]].close()
  }

  override def getPartitionLengthMap: Array[Long] = partitionLengthMap

  override def stop(): Unit = {}
}
