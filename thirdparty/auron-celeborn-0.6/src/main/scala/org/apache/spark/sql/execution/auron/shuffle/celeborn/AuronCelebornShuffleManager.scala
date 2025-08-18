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
import org.apache.celeborn.common.CelebornConf
import org.apache.commons.lang3.reflect.FieldUtils
import org.apache.spark.{ShuffleDependency, SparkConf, TaskContext}
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.celeborn.{CelebornShuffleHandle, ExecutorShuffleIdTracker, SparkShuffleManager}
import org.apache.spark.sql.execution.auron.shuffle.{AuronRssShuffleManagerBase, AuronRssShuffleReaderBase, AuronRssShuffleWriterBase}

class AuronCelebornShuffleManager(conf: SparkConf, isDriver: Boolean)
    extends AuronRssShuffleManagerBase(conf) {
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

  override def getAuronRssShuffleReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): AuronRssShuffleReaderBase[K, C] = {
    this.getAuronRssShuffleReader(
      handle,
      0,
      Int.MaxValue,
      startPartition,
      endPartition,
      context,
      metrics)
  }

  override def getAuronRssShuffleReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): AuronRssShuffleReaderBase[K, C] = {

    val celebornHandle = handle.asInstanceOf[CelebornShuffleHandle[_, _, _]]
    val celebornConf = FieldUtils
      .readField(celebornShuffleManager, "celebornConf", true)
      .asInstanceOf[CelebornConf]
    val shuffleIdTracker = FieldUtils
      .readField(celebornShuffleManager, "shuffleIdTracker", true)
      .asInstanceOf[ExecutorShuffleIdTracker]
    val reader = new AuronCelebornShuffleReader(
      celebornConf,
      celebornHandle,
      startPartition,
      endPartition,
      startMapIndex = Some(startMapIndex),
      endMapIndex = Some(endMapIndex),
      context,
      metrics,
      shuffleIdTracker)
    reader.asInstanceOf[AuronRssShuffleReaderBase[K, C]]
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

  override def getAuronRssShuffleWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): AuronRssShuffleWriterBase[K, V] = {

    val celebornShuffleWriter =
      celebornShuffleManager.getWriter[K, V](handle, mapId, context, metrics)
    val shuffleClient = FieldUtils
      .readField(celebornShuffleManager, "shuffleClient", true)
      .asInstanceOf[ShuffleClient]

    val celebornHandle = handle.asInstanceOf[CelebornShuffleHandle[K, V, _]]
    val shuffleIdTracker = FieldUtils
      .readField(celebornShuffleManager, "shuffleIdTracker", true)
      .asInstanceOf[ExecutorShuffleIdTracker]
    new AuronCelebornShuffleWriter[K, V](
      celebornShuffleWriter,
      shuffleClient,
      context,
      celebornHandle,
      metrics,
      shuffleIdTracker)
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

object AuronCelebornShuffleManager {
  def getEncodedAttemptNumber(context: TaskContext): Int =
    (context.stageAttemptNumber << 16) | context.attemptNumber
}
