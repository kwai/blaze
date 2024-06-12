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

import org.apache.spark.ShuffleDependency
import org.apache.spark.SparkConf
import org.apache.spark.SparkEnv
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.execution.blaze.shuffle.BlazeShuffleDependency.isArrowShuffle

class BlazeShuffleManager(conf: SparkConf) extends ShuffleManager with Logging {
  val sortShuffleManager = new SortShuffleManager(conf)

  if (!conf.getBoolean("spark.shuffle.spill", defaultValue = true)) {
    logWarning(
      "spark.shuffle.spill was set to false, but this configuration is ignored as of Spark 1.6+." +
        " Shuffle will continue to spill to disk when necessary.")
  }

  override val shuffleBlockResolver = new IndexShuffleBlockResolver(conf)

  /**
   * (override) Obtains a [[ShuffleHandle]] to pass to tasks.
   */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    sortShuffleManager.registerShuffle(shuffleId, dependency)
  }

  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {

    if (isArrowShuffle(handle)) {
      new BlazeBlockStoreShuffleReader(
        handle.asInstanceOf[BaseShuffleHandle[K, _, C]],
        startPartition,
        endPartition,
        context,
        metrics,
        SparkEnv.get.blockManager,
        SparkEnv.get.mapOutputTracker,
        startMapId = Some(startMapIndex),
        endMapId = Some(endMapIndex))
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

  /** Get a writer for a given partition. Called on executors by map tasks. */
  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {

    if (isArrowShuffle(handle)) {
      new BlazeShuffleWriter(metrics)
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
