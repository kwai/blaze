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

import org.apache.spark.{ShuffleDependency, SparkConf, SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.shuffle.sort.SortShuffleManager.canUseBatchFetch
import org.apache.spark.sql.execution.auron.shuffle.AuronShuffleDependency.isArrowShuffle

import org.apache.auron.sparkver

class AuronShuffleManager(conf: SparkConf) extends ShuffleManager with Logging {
  val sortShuffleManager = new SortShuffleManager(conf)

  // disable other off-heap memory usages
  System.setProperty("spark.memory.offHeap.enabled", "false")
  System.setProperty("io.netty.maxDirectMemory", "0")
  System.setProperty("io.netty.noPreferDirect", "true")
  System.setProperty("io.netty.noUnsafe", "true")

  if (!conf.getBoolean("spark.shuffle.spill", defaultValue = true)) {
    logWarning(
      "spark.shuffle.spill was set to false, but this configuration is ignored as of Spark 1.6+." +
        " Shuffle will continue to spill to disk when necessary.")
  }

  override val shuffleBlockResolver: ShuffleBlockResolver =
    sortShuffleManager.shuffleBlockResolver

  /**
   * (override) Obtains a [[ShuffleHandle]] to pass to tasks.
   */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    sortShuffleManager.registerShuffle(shuffleId, dependency)
  }

  @sparkver("3.2 / 3.3 / 3.4 / 3.5")
  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {

    if (isArrowShuffle(handle)) {
      val baseShuffleHandle = handle.asInstanceOf[BaseShuffleHandle[K, _, C]]

      @sparkver("3.2")
      def shuffleMergeFinalized = baseShuffleHandle.dependency.shuffleMergeFinalized
      @sparkver("3.3 / 3.4 / 3.5")
      def shuffleMergeFinalized = baseShuffleHandle.dependency.isShuffleMergeFinalizedMarked

      val (blocksByAddress, canEnableBatchFetch) =
        if (shuffleMergeFinalized) {
          val res = SparkEnv.get.mapOutputTracker.getPushBasedShuffleMapSizesByExecutorId(
            handle.shuffleId,
            startMapIndex,
            endMapIndex,
            startPartition,
            endPartition)
          (res.iter, res.enableBatchFetch)
        } else {
          val address = SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(
            handle.shuffleId,
            startMapIndex,
            endMapIndex,
            startPartition,
            endPartition)
          (address, true)
        }

      new AuronBlockStoreShuffleReader(
        handle.asInstanceOf[BaseShuffleHandle[K, _, C]],
        blocksByAddress.map(tup => (tup._1, tup._2.toSeq)),
        context,
        metrics,
        SparkEnv.get.blockManager,
        SparkEnv.get.mapOutputTracker,
        shouldBatchFetch =
          canEnableBatchFetch && canUseBatchFetch(startPartition, endPartition, context))
    } else {
      sortShuffleManager.getReader(
        handle,
        startMapIndex,
        endMapIndex,
        startPartition,
        endPartition,
        context,
        metrics)
    }
  }

  @sparkver("3.1")
  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {

    if (isArrowShuffle(handle)) {
      val address = SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(
        handle.shuffleId,
        startMapIndex,
        endMapIndex,
        startPartition,
        endPartition)
      new AuronBlockStoreShuffleReader(
        handle.asInstanceOf[BaseShuffleHandle[K, _, C]],
        address,
        context,
        metrics,
        SparkEnv.get.blockManager,
        SparkEnv.get.mapOutputTracker,
        shouldBatchFetch = canUseBatchFetch(startPartition, endPartition, context))
    } else {
      sortShuffleManager.getReader(
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

    val blocksByAddress = SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(
      handle.shuffleId,
      startPartition,
      endPartition)

    if (isArrowShuffle(handle)) {
      new AuronBlockStoreShuffleReader(
        handle.asInstanceOf[BaseShuffleHandle[K, _, C]],
        blocksByAddress,
        context,
        metrics,
        SparkEnv.get.blockManager,
        SparkEnv.get.mapOutputTracker,
        shouldBatchFetch = canUseBatchFetch(startPartition, endPartition, context))
    } else {
      sortShuffleManager.getReader(handle, startPartition, endPartition, context, metrics)
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
    val blocksByAddress = SparkEnv.get.mapOutputTracker.getMapSizesByRange(
      handle.shuffleId,
      startMapIndex,
      endMapIndex,
      startPartition,
      endPartition)

    if (isArrowShuffle(handle)) {
      new AuronBlockStoreShuffleReader(
        handle.asInstanceOf[BaseShuffleHandle[K, _, C]],
        blocksByAddress,
        context,
        metrics,
        SparkEnv.get.blockManager,
        SparkEnv.get.mapOutputTracker,
        shouldBatchFetch = canUseBatchFetch(startPartition, endPartition, context))
    } else {
      new BlockStoreShuffleReader(
        handle.asInstanceOf[BaseShuffleHandle[K, _, C]],
        blocksByAddress,
        context,
        metrics,
        shouldBatchFetch = canUseBatchFetch(startPartition, endPartition, context))
    }
  }

  /** Get a writer for a given partition. Called on executors by map tasks. */
  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {

    if (isArrowShuffle(handle)) {
      new AuronShuffleWriter(metrics)
    } else {
      sortShuffleManager.getWriter(handle, mapId, context, metrics)
    }
  }

  /** Remove a shuffle's metadata from the ShuffleManager. */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    sortShuffleManager.unregisterShuffle(shuffleId)
  }

  /** Shut down this ShuffleManager. */
  override def stop(): Unit = {
    shuffleBlockResolver.stop()
  }
}
