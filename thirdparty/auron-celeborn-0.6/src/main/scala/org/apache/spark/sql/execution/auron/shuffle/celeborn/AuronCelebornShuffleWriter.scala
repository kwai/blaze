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

import org.apache.celeborn.client.ShuffleClient
import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.shuffle.ShuffleWriter
import org.apache.spark.shuffle.celeborn.CelebornShuffleHandle
import org.apache.spark.shuffle.celeborn.ExecutorShuffleIdTracker
import org.apache.spark.shuffle.celeborn.SparkUtils
import org.apache.spark.sql.auron.Shims
import org.apache.spark.sql.execution.auron.shuffle.AuronRssShuffleWriterBase
import org.apache.spark.sql.execution.auron.shuffle.RssPartitionWriterBase

import org.apache.auron.sparkver

class AuronCelebornShuffleWriter[K, V](
    celebornShuffleWriter: ShuffleWriter[K, V],
    shuffleClient: ShuffleClient,
    taskContext: TaskContext,
    handle: CelebornShuffleHandle[K, V, _],
    metrics: ShuffleWriteMetricsReporter,
    shuffleIdTracker: ExecutorShuffleIdTracker)
    extends AuronRssShuffleWriterBase[K, V](metrics) {

  private val numMappers = handle.numMappers
  private val encodedAttemptId = AuronCelebornShuffleManager.getEncodedAttemptNumber(taskContext)
  private var celebornPartitionWriter: CelebornPartitionWriter = _
  private var mapId = -1

  override def getRssPartitionWriter(
      _handle: ShuffleHandle,
      _mapId: Int,
      metrics: ShuffleWriteMetricsReporter,
      numPartitions: Int): RssPartitionWriterBase = {
    mapId = _mapId
    val shuffleId = SparkUtils.celebornShuffleId(shuffleClient, handle, taskContext, true)
    shuffleIdTracker.track(handle.shuffleId, shuffleId)
    celebornPartitionWriter = new CelebornPartitionWriter(
      shuffleClient,
      shuffleId,
      encodedAttemptId,
      numMappers,
      numPartitions,
      metrics)
    celebornPartitionWriter
  }

  @sparkver("3.2 / 3.3 / 3.4 / 3.5")
  override def getPartitionLengths(): Array[Long] = {
    celebornPartitionWriter.getPartitionLengthMap
  }

  override def rssStop(success: Boolean): Option[MapStatus] = {
    celebornShuffleWriter.write(Iterator.empty) // force flush
    celebornShuffleWriter.stop(success)
    val blockManagerId = SparkEnv.get.blockManager.shuffleServerId
    Some(
      Shims.get
        .getMapStatus(blockManagerId, celebornPartitionWriter.getPartitionLengthMap, mapId))
  }
}
