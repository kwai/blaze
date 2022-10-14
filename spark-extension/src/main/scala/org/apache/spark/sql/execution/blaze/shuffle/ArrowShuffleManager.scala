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

import org.apache.spark.ShuffleDependency
import org.apache.spark.SparkConf
import org.apache.spark.SparkEnv
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.sort.ArrowShuffleWriter
import org.apache.spark.shuffle.sort.BypassMergeSortShuffleHandle
import org.apache.spark.shuffle.sort.SerializedShuffleHandle
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.blaze.kwai.KwaiPrivilegedHDFSBlockManager

class ArrowShuffleManager(conf: SparkConf) extends ShuffleManager with Logging {
  import ArrowShuffleManager._

  // introduce a wrapped HDFSBlockManager because we found some permission issues
  // if the original HDFSBlockManager is initialized inside a native thread.
  KwaiPrivilegedHDFSBlockManager.setup(conf)

  val sortShuffleManager = new SortShuffleManager(conf)

  override val shuffleBlockResolver: IndexShuffleBlockResolver =
    sortShuffleManager.shuffleBlockResolver

  /**
   * A mapping from shuffle ids to the number of mappers producing output for those shuffles.
   */
  private[this] val numMapsForShuffle: ConcurrentHashMap[Int, Int] = {
    val field = sortShuffleManager.getClass.getDeclaredField("numMapsForShuffle")
    field.setAccessible(true)
    field.get(sortShuffleManager).asInstanceOf[ConcurrentHashMap[Int, Int]]
  }

  /**
   * Obtains a [[ShuffleHandle]] to pass to tasks.
   */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      numMaps: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    sortShuffleManager.registerShuffle(shuffleId, numMaps, dependency)
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
        metrics)
    } else {
      sortShuffleManager.getReader(handle, startPartition, endPartition, context, metrics)
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
      new ArrowBlockStoreShuffleReader(
        handle.asInstanceOf[BaseShuffleHandle[K, _, C]],
        startPartition,
        endPartition,
        context,
        metrics,
        startMapId = Some(startMapId),
        endMapId = Some(endMapId))
    } else {
      sortShuffleManager.getReader(
        handle,
        startPartition,
        endPartition,
        context,
        metrics,
        startMapId,
        endMapId)
    }
  }

  /** Get a writer for a given partition. Called on executors by map tasks. */
  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Int,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {

    if (isArrowShuffle(handle)) {
      val env = SparkEnv.get
      numMapsForShuffle.putIfAbsent(
        handle.shuffleId,
        handle.asInstanceOf[BaseShuffleHandle[_, _, _]].numMaps)

      handle match {
        case unsafeShuffleHandle: SerializedShuffleHandle[K @unchecked, V @unchecked] =>
          new ArrowShuffleWriter(
            env.blockManager,
            shuffleBlockResolver,
            context.taskMemoryManager(),
            unsafeShuffleHandle,
            mapId,
            context,
            env.conf,
            metrics)
        case bypassMergeSortHandle: BypassMergeSortShuffleHandle[K @unchecked, V @unchecked] =>
          new ArrowBypassMergeSortShuffleWriter(
            env.blockManager,
            shuffleBlockResolver,
            bypassMergeSortHandle,
            mapId,
            context,
            env.conf,
            metrics)
        case _ =>
          throw new RuntimeException(s"unsupported shuffle handle type: ${handle.getClass}")
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
    sortShuffleManager.stop()
  }
}

private[spark] object ArrowShuffleManager extends Logging {

  private def isArrowShuffle(handle: ShuffleHandle): Boolean = {
    val base = handle.asInstanceOf[BaseShuffleHandle[_, _, _]]
    val dep = base.dependency
    dep.isInstanceOf[ArrowShuffleDependency[_, _, _]]
  }

  lazy val compressionCodecForShuffling: CompressionCodec = {
    val sparkConf = SparkEnv.get.conf
    val zcodecConfName = "spark.blaze.shuffle.compression.codec"
    val zcodecName =
      sparkConf.get(zcodecConfName, defaultValue = sparkConf.get("spark.io.compression.codec"))

    // only zstd compression is supported at the moment
    if (zcodecName != "zstd") {
      logWarning(
        s"Overriding config spark.io.compression.codec=${zcodecName} in shuffling, force using zstd")
    }
    CompressionCodec.createCodec(sparkConf, "zstd")
  }
}
