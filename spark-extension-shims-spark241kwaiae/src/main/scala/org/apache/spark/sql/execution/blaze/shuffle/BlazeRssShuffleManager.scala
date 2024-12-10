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

import java.util.concurrent.ConcurrentHashMap

import com.kuaishou.dataarch.shuffle.proto.dto.common.PartitionStatistics
import org.apache.spark.SparkConf
import org.apache.spark.TaskContext

import org.apache.spark.shuffle._
import org.apache.spark.shuffle.stream.StreamShuffleHandle
import org.apache.spark.shuffle.stream.StreamShuffleManager
import org.apache.spark.sql.blaze.kwai.KwaiPrivilegedHDFSBlockManager
import org.apache.spark.sql.execution.blaze.shuffle.BlazeShuffleDependency.isArrowShuffle

class BlazeRssShuffleManager(conf: SparkConf) extends UnifiedShuffleManager(conf) {

  // introduce a wrapped HDFSBlockManager because we found some permission issues
  // if the original HDFSBlockManager is initialized inside a native thread.
  KwaiPrivilegedHDFSBlockManager.setup(conf)

  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
   * Called on executors by reduce tasks.
   */
  override def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {

    if (isArrowShuffle(handle)) {
      new BlazeRssBlockStoreShuffleReader[K, C](
        handle.asInstanceOf[StreamShuffleHandle[K, C]],
        startPartition,
        endPartition,
        context,
        metrics)
    } else {
      getEffectiveManager(handle).getReader(
        handle,
        startPartition,
        endPartition,
        context,
        metrics)
    }
  }

  override def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter,
      startMapId: Int,
      endMapId: Int): ShuffleReader[K, C] = {

    if (isArrowShuffle(handle)) {
      new BlazeRssBlockStoreShuffleReader[K, C](
        handle.asInstanceOf[StreamShuffleHandle[K, C]],
        startPartition,
        endPartition,
        context,
        metrics,
        startMapId = Some(startMapId),
        endMapId = Some(endMapId))
    } else {
      getEffectiveManager(handle).getReader(
        handle,
        startPartition,
        endPartition,
        context,
        metrics,
        startMapId,
        endMapId)
    }
  }

  override def getRebalancedReader[K, C](
      handle: ShuffleHandle,
      partitionId: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter,
      partitionStatistics: Seq[PartitionStatistics]): ShuffleReader[K, C] = {
    if (isArrowShuffle(handle)) {
      new BlazeRssBlockStoreShuffleReader[K, C](
        handle.asInstanceOf[StreamShuffleHandle[K, C]],
        partitionId,
        partitionId + 1,
        context,
        metrics,
        startMapId = None,
        endMapId = None,
        partitionStatistics = Some(partitionStatistics))
    } else {
      getEffectiveManager(handle)
        .asInstanceOf[StreamShuffleManager]
        .getRebalancedReader(handle, partitionId, context, metrics, partitionStatistics)
    }
  }

  /** Get a writer for a given partition. Called on executors by map tasks. */
  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Int,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    if (isArrowShuffle(handle)) {
      new BlazeRssShuffleWriter[K, V](metrics)
    } else {
      getEffectiveManager(handle).getWriter(handle, mapId, context, metrics)
    }

  }
}
