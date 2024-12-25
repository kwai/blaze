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
import org.apache.celeborn.common.CelebornConf
import org.apache.commons.lang3.reflect.FieldUtils
import org.apache.spark.sql.execution.blaze.shuffle.BlazeRssShuffleManagerBase
import org.apache.spark.SparkConf
import org.apache.spark.TaskContext
import org.apache.spark.shuffle.ShuffleBlockResolver
import org.apache.spark.shuffle.ShuffleReader
import org.apache.spark.shuffle.ShuffleWriter
import org.apache.spark.sql.execution.blaze.shuffle.BlazeRssShuffleWriterBase
import org.apache.spark.ShuffleDependency
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.shuffle.ShuffleReadMetricsReporter
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.shuffle.celeborn.SparkShuffleManager
import org.apache.spark.sql.execution.blaze.shuffle.BlazeRssShuffleReaderBase
import org.apache.spark.shuffle.celeborn.CelebornShuffleHandle
import org.apache.spark.shuffle.celeborn.ExecutorShuffleIdTracker

class BlazeCelebornShuffleManager(conf: SparkConf, isDriver: Boolean)
    extends BlazeRssShuffleManagerBase(conf) {
  private val celebornShuffleManager: SparkShuffleManager =
    new SparkShuffleManager(conf, isDriver)

  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    celebornShuffleManager.registerShuffle(shuffleId, dependency)
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    celebornShuffleManager.unregisterShuffle(shuffleId)
  }

  override def getBlazeRssShuffleReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): BlazeRssShuffleReaderBase[K, C] = {
    this.getBlazeRssShuffleReader(
      handle,
      0,
      Int.MaxValue,
      startPartition,
      endPartition,
      context,
      metrics)
  }

  override def getBlazeRssShuffleReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): BlazeRssShuffleReaderBase[K, C] = {

    val celebornHandle = handle.asInstanceOf[CelebornShuffleHandle[_, _, _]]
    val celebornConf = FieldUtils
      .readField(celebornShuffleManager, "celebornConf", true)
      .asInstanceOf[CelebornConf]
    val shuffleIdTracker = FieldUtils
      .readField(celebornShuffleManager, "shuffleIdTracker", true)
      .asInstanceOf[ExecutorShuffleIdTracker]
    val reader = new BlazeCelebornShuffleReader(
      celebornConf,
      celebornHandle,
      startPartition,
      endPartition,
      startMapIndex = Some(startMapIndex),
      endMapIndex = Some(endMapIndex),
      context,
      metrics,
      shuffleIdTracker)
    reader.asInstanceOf[BlazeRssShuffleReaderBase[K, C]]
  }

  override def getRssShuffleReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    celebornShuffleManager.getReader(handle, startPartition, endPartition, context, metrics)
  }

  override def getRssShuffleReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    celebornShuffleManager.getReaderForRange(
      handle,
      startMapIndex,
      endMapIndex,
      startPartition,
      endPartition,
      context,
      metrics)
  }

  override def getBlazeRssShuffleWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): BlazeRssShuffleWriterBase[K, V] = {

    // ensure celeborn client is initialized
    assert(celebornShuffleManager.getWriter(handle, mapId, context, metrics) != null)
    val shuffleClient = FieldUtils
      .readField(celebornShuffleManager, "shuffleClient", true)
      .asInstanceOf[ShuffleClient]

    val celebornHandle = handle.asInstanceOf[CelebornShuffleHandle[_, _, _]]
    val shuffleIdTracker = FieldUtils
      .readField(celebornShuffleManager, "shuffleIdTracker", true)
      .asInstanceOf[ExecutorShuffleIdTracker]
    val writer = new BlazeCelebornShuffleWriter(shuffleClient, context, celebornHandle, metrics, shuffleIdTracker)
    writer.asInstanceOf[BlazeRssShuffleWriterBase[K, V]]
  }

  override def getRssShuffleWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    celebornShuffleManager.getWriter(handle, mapId, context, metrics)
  }

  override def shuffleBlockResolver: ShuffleBlockResolver =
    celebornShuffleManager.shuffleBlockResolver()

  override def stop(): Unit =
    celebornShuffleManager.stop()
}

object BlazeCelebornShuffleManager {
  def getEncodedAttemptNumber(context: TaskContext): Int =
    (context.stageAttemptNumber << 16) | context.attemptNumber
}
