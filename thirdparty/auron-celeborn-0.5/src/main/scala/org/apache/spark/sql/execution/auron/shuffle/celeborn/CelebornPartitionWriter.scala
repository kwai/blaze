/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.auron.shuffle.celeborn

import java.nio.ByteBuffer

import org.apache.celeborn.client.{ShuffleClient, ShuffleClientImpl}
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.sql.execution.auron.shuffle.RssPartitionWriterBase

class CelebornPartitionWriter(
    shuffleClient: ShuffleClient,
    shuffleId: Int,
    encodedAttemptId: Int,
    numMappers: Int,
    numPartitions: Int,
    metrics: ShuffleWriteMetricsReporter)
    extends RssPartitionWriterBase
    with Logging {

  private val mapStatusLengths: Array[Long] = Array.fill(numPartitions)(0L)
  private val mapId = TaskContext.get.partitionId

  override def write(partitionId: Int, buffer: ByteBuffer): Unit = {
    val numBytes = buffer.limit()
    val bytes = new Array[Byte](numBytes)
    buffer.get(bytes)
    val bytesWritten = shuffleClient
      .asInstanceOf[ShuffleClientImpl]
      .pushOrMergeData(
        shuffleId,
        mapId,
        encodedAttemptId,
        partitionId,
        bytes,
        0,
        numBytes,
        numMappers,
        numPartitions,
        true, // doPush
        true // skipCompress
      )
    metrics.incBytesWritten(bytesWritten)
    mapStatusLengths(partitionId) += bytesWritten
  }

  override def flush(): Unit = {}

  override def close(success: Boolean): Unit = {
    val waitStartTime = System.nanoTime()
    if (success) {
      shuffleClient.mapperEnd(shuffleId, mapId, encodedAttemptId, numMappers)
    }
    shuffleClient.cleanup(shuffleId, mapId, encodedAttemptId)
    metrics.incWriteTime(System.nanoTime() - waitStartTime)
  }

  override def getPartitionLengthMap: Array[Long] =
    mapStatusLengths
}
