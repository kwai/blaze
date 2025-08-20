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
package org.apache.spark.sql.execution.auron.shuffle

import org.apache.spark.{ShuffleDependency, SparkConf, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle._
import org.apache.spark.sql.execution.auron.shuffle.AuronShuffleDependency.isArrowShuffle

import org.apache.auron.sparkver

abstract class AuronRssShuffleManagerBase(_conf: SparkConf) extends ShuffleManager with Logging {
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle

  override def unregisterShuffle(shuffleId: Int): Boolean

  def getAuronRssShuffleReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): AuronRssShuffleReaderBase[K, C]

  def getAuronRssShuffleReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): AuronRssShuffleReaderBase[K, C]

  def getRssShuffleReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C]

  def getRssShuffleReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C]

  def getAuronRssShuffleWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): AuronRssShuffleWriterBase[K, V]

  def getRssShuffleWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V]

  @sparkver("3.1 / 3.2 / 3.3 / 3.4 / 3.5")
  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {

    if (isArrowShuffle(handle)) {
      getAuronRssShuffleReader(
        handle,
        startMapIndex,
        endMapIndex,
        startPartition,
        endPartition,
        context,
        metrics)
    } else {
      getRssShuffleReader(
        handle,
        startMapIndex,
        endMapIndex,
        startPartition,
        endPartition,
        context,
        metrics)
    }
  }

  @sparkver("3.0")
  override def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {

    if (isArrowShuffle(handle)) {
      getAuronRssShuffleReader(handle, startPartition, endPartition, context, metrics)
    } else {
      getRssShuffleReader(handle, startPartition, endPartition, context, metrics)
    }
  }

  @sparkver("3.0")
  override def getReaderForRange[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {

    if (isArrowShuffle(handle)) {
      getAuronRssShuffleReader(
        handle,
        startMapIndex,
        endMapIndex,
        startPartition,
        endPartition,
        context,
        metrics)
    } else {
      getRssShuffleReader(
        handle,
        startMapIndex,
        endMapIndex,
        startPartition,
        endPartition,
        context,
        metrics)
    }
  }

  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {

    if (isArrowShuffle(handle)) {
      getAuronRssShuffleWriter(handle, mapId, context, metrics)
    } else {
      getRssShuffleWriter(handle, mapId, context, metrics)
    }
  }
}
