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
package org.apache.spark.sql.execution.auron.shuffle.uniffle

import org.apache.spark.{ShuffleDependency, SparkConf, TaskContext}
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.reader.RssShuffleReader
import org.apache.spark.shuffle.uniffle.RssShuffleHandleWrapper
import org.apache.spark.shuffle.writer.RssShuffleWriter
import org.apache.spark.sql.execution.auron.shuffle.{AuronRssShuffleManagerBase, AuronRssShuffleReaderBase, AuronRssShuffleWriterBase}

class AuronUniffleShuffleManager(conf: SparkConf, isDriver: Boolean)
    extends AuronRssShuffleManagerBase(conf) {
  private val uniffleShuffleManager: RssShuffleManager = new RssShuffleManager(conf, isDriver);
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    val handle = uniffleShuffleManager.registerShuffle(shuffleId, dependency)
    new RssShuffleHandleWrapper(handle.asInstanceOf[RssShuffleHandle[K, V, C]])
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    uniffleShuffleManager.unregisterShuffle(shuffleId)
  }

  override def getAuronRssShuffleReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): AuronRssShuffleReaderBase[K, C] = {
    getAuronRssShuffleReader(
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
    val rssHandleWrapper = handle.asInstanceOf[RssShuffleHandleWrapper[K, _, C]]
    val reader =
      uniffleShuffleManager.getReader(
        rssHandleWrapper.rssShuffleHandleInfo,
        startMapIndex,
        endMapIndex,
        startPartition,
        endPartition,
        context,
        metrics)
    new AuronUniffleShuffleReader(
      reader.asInstanceOf[RssShuffleReader[K, C]],
      rssHandleWrapper,
      startMapIndex,
      endMapIndex,
      startPartition,
      endPartition,
      context,
      metrics)
  }

  override def getRssShuffleReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    val rssHandle = handle.asInstanceOf[RssShuffleHandleWrapper[K, _, C]].rssShuffleHandleInfo
    uniffleShuffleManager.getReader(rssHandle, startPartition, endPartition, context, metrics)
  }

  override def getRssShuffleReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    val rssHandle = handle.asInstanceOf[RssShuffleHandleWrapper[K, _, C]].rssShuffleHandleInfo
    uniffleShuffleManager.getReader(
      rssHandle,
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
    val rssHandle = handle.asInstanceOf[RssShuffleHandleWrapper[K, _, V]].rssShuffleHandleInfo
    val writer: ShuffleWriter[K, V] =
      uniffleShuffleManager.getWriter(rssHandle, mapId, context, metrics)
    new AuronUniffleShuffleWriter(writer.asInstanceOf[RssShuffleWriter[K, V, _]], metrics)
  }

  override def getRssShuffleWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    val rssHandle = handle.asInstanceOf[RssShuffleHandleWrapper[K, _, V]].rssShuffleHandleInfo
    uniffleShuffleManager.getWriter(rssHandle, mapId, context, metrics)
  }

  override def shuffleBlockResolver: ShuffleBlockResolver = {
    uniffleShuffleManager.shuffleBlockResolver()
  }

  override def stop(): Unit = {
    uniffleShuffleManager.stop()
  }
}
