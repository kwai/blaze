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

import scala.collection.JavaConverters._

import org.apache.spark.ShuffleDependency
import org.apache.spark.SparkConf
import org.apache.spark.SparkEnv
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.api.ShuffleExecutorComponents
import org.apache.spark.shuffle.sort.ArrowShuffleWriter
import org.apache.spark.shuffle.sort.BypassMergeSortShuffleHandle
import org.apache.spark.shuffle.sort.SerializedShuffleHandle
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.execution.blaze.shuffle.ArrowShuffleDependency.isArrowShuffle
import org.apache.spark.util.collection.OpenHashSet

class ArrowShuffleManager(conf: SparkConf) extends ShuffleManager with Logging {

  import ArrowShuffleManager._
  import SortShuffleManager._

  val sortShuffleManager = new SortShuffleManager(conf)

  if (!conf.getBoolean("spark.shuffle.spill", defaultValue = true)) {
    logWarning(
      "spark.shuffle.spill was set to false, but this configuration is ignored as of Spark 1.6+." +
        " Shuffle will continue to spill to disk when necessary.")
  }

  private lazy val shuffleExecutorComponents = loadShuffleExecutorComponents(conf)
  override val shuffleBlockResolver = new IndexShuffleBlockResolver(conf)

  /**
   * (override) Obtains a [[ShuffleHandle]] to pass to tasks.
   */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    sortShuffleManager.registerShuffle(shuffleId, dependency)
  }

  protected def registerShuffle[K, V, C](
      shuffleId: Int,
      mapId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    val registerShuffleMethod = classOf[SortShuffleManager].getDeclaredMethod(
      "registerShuffle",
      Integer.TYPE,
      Integer.TYPE,
      classOf[ShuffleDependency[_, _, _]])
    registerShuffleMethod.setAccessible(true)
    registerShuffleMethod
      .invoke(sortShuffleManager, new Integer(shuffleId), new Integer(mapId), dependency)
      .asInstanceOf[ShuffleHandle]
  }

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
      new ArrowBlockStoreShuffleReader(
        handle.asInstanceOf[BaseShuffleHandle[K, _, C]],
        startPartition,
        endPartition,
        context,
        metrics,
        SparkEnv.get.blockManager,
        SparkEnv.get.mapOutputTracker,
        None,
        None,
        shouldBatchFetch = canUseBatchFetch(startPartition, endPartition, context))
    } else {
      sortShuffleManager.getReader(handle, startPartition, endPartition, context, metrics)
    }
  }

  override def getReaderForRange[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {

    if (isArrowShuffle(handle)) {
      new ArrowBlockStoreShuffleReader(
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
      sortShuffleManager.getReaderForRange(
        handle,
        startPartition,
        endPartition,
        startMapIndex,
        endMapIndex,
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
      val env = SparkEnv.get
      handle match {
        case unsafeShuffleHandle: SerializedShuffleHandle[K @unchecked, V @unchecked] =>
          new ArrowShuffleWriter(
            env.blockManager,
            context.taskMemoryManager(),
            unsafeShuffleHandle,
            mapId,
            context,
            env.conf,
            metrics,
            shuffleExecutorComponents)
        case bypassMergeSortHandle: BypassMergeSortShuffleHandle[K @unchecked, V @unchecked] =>
          new ArrowBypassMergeSortShuffleWriter(
            env.blockManager,
            bypassMergeSortHandle,
            mapId,
            env.conf,
            metrics,
            shuffleExecutorComponents)
        case other =>
          throw new UnsupportedOperationException(s"$other type not allowed for arrow-shuffle")
      }
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

private[spark] object ArrowShuffleManager extends Logging {
  private def loadShuffleExecutorComponents(conf: SparkConf): ShuffleExecutorComponents = {
    val executorComponents = ShuffleDataIOUtils.loadShuffleDataIO(conf).executor()
    val extraConfigs = conf.getAllWithPrefix(ShuffleDataIOUtils.SHUFFLE_SPARK_CONF_PREFIX).toMap
    executorComponents.initializeExecutor(
      conf.getAppId,
      SparkEnv.get.executorId,
      extraConfigs.asJava)
    executorComponents
  }
}
