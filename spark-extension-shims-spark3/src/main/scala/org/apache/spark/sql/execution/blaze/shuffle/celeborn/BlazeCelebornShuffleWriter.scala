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
package org.apache.spark.sql.execution.blaze.shuffle.celeborn

import org.apache.celeborn.client.ShuffleClient
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.shuffle.celeborn.{CelebornShuffleHandle, ExecutorShuffleIdTracker, SparkUtils}
import org.apache.spark.sql.execution.blaze.shuffle.BlazeRssShuffleWriterBase
import org.apache.spark.sql.execution.blaze.shuffle.RssPartitionWriterBase
import org.apache.spark.TaskContext

import com.thoughtworks.enableIf

class BlazeCelebornShuffleWriter[K, C](
    shuffleClient: ShuffleClient,
    taskContext: TaskContext,
    handle: CelebornShuffleHandle[K, _, C],
    metrics: ShuffleWriteMetricsReporter,
    shuffleIdTracker: ExecutorShuffleIdTracker)
    extends BlazeRssShuffleWriterBase[K, C](metrics) {

  private val numMappers = handle.numMappers
  private val encodedAttemptId = BlazeCelebornShuffleManager.getEncodedAttemptNumber(taskContext)

  override def getRssPartitionWriter(
      _handle: ShuffleHandle,
      _mapId: Int,
      metrics: ShuffleWriteMetricsReporter,
      numPartitions: Int): RssPartitionWriterBase = {

    val shuffleId = SparkUtils.celebornShuffleId(shuffleClient, handle, taskContext, true)
    shuffleIdTracker.track(handle.shuffleId, shuffleId)
    new CelebornPartitionWriter(
      shuffleClient,
      shuffleId,
      encodedAttemptId,
      numMappers,
      numPartitions,
      metrics)
  }

  @enableIf(
    Seq("spark-3.2", "spark-3.3", "spark-3.4", "spark-3.5").contains(
      System.getProperty("blaze.shim")))
  override def getPartitionLengths(): Array[Long] = partitionLengths

}
