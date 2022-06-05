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

package org.apache.spark.sql.blaze.execution

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.spark.ShuffleDependency
import org.apache.spark.SparkConf
import org.apache.spark.SparkEnv
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.IO_COMPRESSION_CODEC
import org.apache.spark.io.CompressionCodec
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.api.ShuffleExecutorComponents
import org.apache.spark.shuffle.sort.ArrowShuffleWriter301
import org.apache.spark.shuffle.sort.BypassMergeSortShuffleHandle
import org.apache.spark.shuffle.sort.SerializedShuffleHandle
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.shuffle.sort.SortShuffleWriter
import org.apache.spark.util.collection.OpenHashSet

class ArrowShuffleManager301(conf: SparkConf) extends ShuffleManager with Logging {

  import ArrowShuffleManager301._
  import SortShuffleManager._

  if (!conf.getBoolean("spark.shuffle.spill", true)) {
    logWarning(
      "spark.shuffle.spill was set to false, but this configuration is ignored as of Spark 1.6+." +
        " Shuffle will continue to spill to disk when necessary.")
  }

  private lazy val shuffleExecutorComponents = loadShuffleExecutorComponents(conf)
  override val shuffleBlockResolver = new IndexShuffleBlockResolver(conf)

  /**
   * A mapping from shuffle ids to the task ids of mappers producing output for those shuffles.
   */
  private[this] val taskIdMapsForShuffle = new ConcurrentHashMap[Int, OpenHashSet[Long]]()

  /**
   * (override) Obtains a [[ShuffleHandle]] to pass to tasks.
   */
  def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    if (SortShuffleWriter.shouldBypassMergeSort(conf, dependency)) {
      // If there are fewer than spark.shuffle.sort.bypassMergeThreshold partitions and we don't
      // need map-side aggregation, then write numPartitions files directly and just concatenate
      // them at the end. This avoids doing serialization and deserialization twice to merge
      // together the spilled files, which would happen with the normal code path. The downside is
      // having multiple files open at a time and thus more memory allocated to buffers.
      classOf[BypassMergeSortShuffleHandle[K, V]]
        .getConstructor(Integer.TYPE, classOf[ShuffleDependency[_, _, _]])
        .newInstance(Int.box(shuffleId), dependency.asInstanceOf[ShuffleDependency[_, _, _]])
        .asInstanceOf[ShuffleHandle]

    } else if (SortShuffleManager.canUseSerializedShuffle(dependency)) {
      // Otherwise, try to buffer map outputs in a serialized form, since this is more efficient:
      classOf[SerializedShuffleHandle[K, V]]
        .getConstructor(Integer.TYPE, classOf[ShuffleDependency[_, _, _]])
        .newInstance(Int.box(shuffleId), dependency.asInstanceOf[ShuffleDependency[_, _, _]])
        .asInstanceOf[ShuffleHandle]

    } else {
      // Otherwise, buffer map outputs in a deserialized form:
      classOf[BaseShuffleHandle[K, V, C]]
        .getConstructor(Integer.TYPE, classOf[ShuffleDependency[_, _, _]])
        .newInstance(Int.box(shuffleId), dependency.asInstanceOf[ShuffleDependency[_, _, _]])
        .asInstanceOf[ShuffleHandle]
    }
  }

  /**
   * Obtains a [[ShuffleHandle]] to pass to tasks.
   */
  def registerShuffle[K, V, C](
      shuffleId: Int,
      numMaps: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    if (SortShuffleWriter.shouldBypassMergeSort(conf, dependency)) {
      // If there are fewer than spark.shuffle.sort.bypassMergeThreshold partitions and we don't
      // need map-side aggregation, then write numPartitions files directly and just concatenate
      // them at the end. This avoids doing serialization and deserialization twice to merge
      // together the spilled files, which would happen with the normal code path. The downside is
      // having multiple files open at a time and thus more memory allocated to buffers.
      classOf[BypassMergeSortShuffleHandle[K, V]]
        .getConstructor(Integer.TYPE, Integer.TYPE, classOf[ShuffleDependency[_, _, _]])
        .newInstance(
          Int.box(shuffleId),
          Int.box(numMaps),
          dependency.asInstanceOf[ShuffleDependency[_, _, _]])
        .asInstanceOf[ShuffleHandle]

    } else if (SortShuffleManager.canUseSerializedShuffle(dependency)) {
      // Otherwise, try to buffer map outputs in a serialized form, since this is more efficient:
      classOf[SerializedShuffleHandle[K, V]]
        .getConstructor(Integer.TYPE, Integer.TYPE, classOf[ShuffleDependency[_, _, _]])
        .newInstance(
          Int.box(shuffleId),
          Int.box(numMaps),
          dependency.asInstanceOf[ShuffleDependency[_, _, _]])
        .asInstanceOf[ShuffleHandle]

    } else {
      // Otherwise, buffer map outputs in a deserialized form:
      classOf[BaseShuffleHandle[K, V, C]]
        .getConstructor(Integer.TYPE, Integer.TYPE, classOf[ShuffleDependency[_, _, _]])
        .newInstance(
          Int.box(shuffleId),
          Int.box(numMaps),
          dependency.asInstanceOf[ShuffleDependency[_, _, _]])
        .asInstanceOf[ShuffleHandle]
    }
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
    val blocksByAddress = () =>
      SparkEnv.get.mapOutputTracker
        .getMapSizesByExecutorId(handle.shuffleId, startPartition, endPartition)
    new ArrowBlockStoreShuffleReader301(
      handle.asInstanceOf[BaseShuffleHandle[K, _, C]],
      blocksByAddress,
      context,
      metrics,
      shouldBatchFetch = canUseBatchFetch(startPartition, endPartition, context))
  }

  override def getReaderForRange[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    val blocksByAddress = () =>
      SparkEnv.get.mapOutputTracker.getMapSizesByRange(
        handle.shuffleId,
        startMapIndex,
        endMapIndex,
        startPartition,
        endPartition)
    new ArrowBlockStoreShuffleReader301(
      handle.asInstanceOf[BaseShuffleHandle[K, _, C]],
      blocksByAddress,
      context,
      metrics,
      shouldBatchFetch = canUseBatchFetch(startPartition, endPartition, context))
  }

  /** Get a writer for a given partition. Called on executors by map tasks. */
  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    val mapTaskIds =
      taskIdMapsForShuffle.computeIfAbsent(handle.shuffleId, _ => new OpenHashSet[Long](16))
    mapTaskIds.synchronized {
      mapTaskIds.add(context.taskAttemptId())
    }
    val env = SparkEnv.get
    handle match {
      case unsafeShuffleHandle: SerializedShuffleHandle[K @unchecked, V @unchecked] =>
        require(unsafeShuffleHandle.dependency.isInstanceOf[ShuffleDependencySchema[K, V, _]])
        new ArrowShuffleWriter301(
          env.blockManager,
          context.taskMemoryManager(),
          unsafeShuffleHandle,
          mapId,
          context,
          env.conf,
          metrics,
          shuffleExecutorComponents)
      case bypassMergeSortHandle: BypassMergeSortShuffleHandle[K @unchecked, V @unchecked] =>
        require(bypassMergeSortHandle.dependency.isInstanceOf[ShuffleDependencySchema[K, V, _]])
        new ArrowBypassMergeSortShuffleWriter301(
          env.blockManager,
          bypassMergeSortHandle,
          mapId,
          env.conf,
          metrics,
          shuffleExecutorComponents)
      case other: BaseShuffleHandle[K @unchecked, V @unchecked, _] =>
        throw new UnsupportedOperationException(s"$other type not allowed for arrow-shuffle")
    }
  }

  /** Remove a shuffle's metadata from the ShuffleManager. */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    Option(taskIdMapsForShuffle.remove(shuffleId)).foreach { mapTaskIds =>
      mapTaskIds.iterator.foreach { mapTaskId =>
        shuffleBlockResolver.removeDataByMap(shuffleId, mapTaskId)
      }
    }
    true
  }

  /** Shut down this ShuffleManager. */
  override def stop(): Unit = {
    shuffleBlockResolver.stop()
  }
}

private[spark] object ArrowShuffleManager301 extends Logging {
  private def loadShuffleExecutorComponents(conf: SparkConf): ShuffleExecutorComponents = {
    val executorComponents = ShuffleDataIOUtils.loadShuffleDataIO(conf).executor()
    val extraConfigs = conf.getAllWithPrefix(ShuffleDataIOUtils.SHUFFLE_SPARK_CONF_PREFIX).toMap
    executorComponents.initializeExecutor(
      conf.getAppId,
      SparkEnv.get.executorId,
      extraConfigs.asJava)
    executorComponents
  }

  lazy val compressionCodecForShuffling: CompressionCodec = {
    val sparkConf = SparkEnv.get.conf
    val zcodecConfName = "spark.blaze.shuffle.compression.codec"
    val zcodecName =
      sparkConf.get(zcodecConfName, defaultValue = sparkConf.get(IO_COMPRESSION_CODEC))

    // only zstd compression is supported at the moment
    if (zcodecName != "zstd") {
      logWarning(
        s"Overriding config ${IO_COMPRESSION_CODEC}=${zcodecName} in shuffling, force using zstd")
    }
    CompressionCodec.createCodec(sparkConf, "zstd")
  }
}
