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

import java.io.InputStream
import java.nio.ByteBuffer
import java.util

import scala.collection.AbstractIterator

import org.apache.commons.lang3.reflect.FieldUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{ShuffleDependency, TaskContext}
import org.apache.spark.executor.ShuffleReadMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ShuffleReadMetricsReporter
import org.apache.spark.shuffle.reader.{RssShuffleDataIterator, RssShuffleReader}
import org.apache.spark.shuffle.uniffle.RssShuffleHandleWrapper
import org.apache.spark.sql.execution.auron.shuffle.AuronRssShuffleReaderBase
import org.apache.spark.util.TaskCompletionListener
import org.apache.uniffle.client.api.ShuffleReadClient
import org.apache.uniffle.client.factory.ShuffleClientFactory
import org.apache.uniffle.client.util.RssClientConfig
import org.apache.uniffle.common.{ShuffleDataDistributionType, ShuffleServerInfo}
import org.apache.uniffle.common.config.RssConf
import org.apache.uniffle.common.exception.RssException
import org.apache.uniffle.shaded.org.roaringbitmap.longlong.Roaring64NavigableMap

class AuronUniffleShuffleReader[K, C](
    reader: RssShuffleReader[K, C],
    handle: RssShuffleHandleWrapper[K, _, C],
    startMapIndex: Int,
    endMapIndex: Int,
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    metrics: ShuffleReadMetricsReporter)
    extends AuronRssShuffleReaderBase[K, C](handle, context)
    with Logging {
  private val numMaps: Int =
    FieldUtils.readField(reader, "numMaps", true).asInstanceOf[Int]
  private val partitionToExpectBlocks: util.Map[Integer, Roaring64NavigableMap] = FieldUtils
    .readField(reader, "partitionToExpectBlocks", true)
    .asInstanceOf[util.Map[Integer, Roaring64NavigableMap]]
  private val partitionToShuffleServers: util.Map[Integer, util.List[ShuffleServerInfo]] =
    FieldUtils
      .readField(reader, "partitionToShuffleServers", true)
      .asInstanceOf[util.Map[Integer, util.List[ShuffleServerInfo]]]
  private val mapStartIndex: Int =
    FieldUtils.readField(reader, "mapStartIndex", true).asInstanceOf[Int]
  private val mapEndIndex: Int =
    FieldUtils.readField(reader, "mapEndIndex", true).asInstanceOf[Int]
  private val rssConf: RssConf =
    FieldUtils.readField(reader, "rssConf", true).asInstanceOf[RssConf]
  private val dependency = FieldUtils
    .readField(reader, "shuffleDependency", true)
    .asInstanceOf[ShuffleDependency[K, _, C]]
  private val appId: String =
    FieldUtils.readField(reader, "appId", true).asInstanceOf[String]
  private val shuffleId: Int =
    FieldUtils.readField(reader, "shuffleId", true).asInstanceOf[Int]
  private val taskId: String =
    FieldUtils.readField(reader, "taskId", true).asInstanceOf[String]
  private val basePath: String =
    FieldUtils.readField(reader, "basePath", true).asInstanceOf[String]
  private val partitionNum: Int =
    FieldUtils.readField(reader, "partitionNum", true).asInstanceOf[Int]
  private val taskIdBitmap: Roaring64NavigableMap = FieldUtils
    .readField(reader, "taskIdBitmap", true)
    .asInstanceOf[Roaring64NavigableMap]
  private val hadoopConf: Configuration =
    FieldUtils.readField(reader, "hadoopConf", true).asInstanceOf[Configuration]
  private val dataDistributionType: ShuffleDataDistributionType = FieldUtils
    .readField(reader, "dataDistributionType", true)
    .asInstanceOf[ShuffleDataDistributionType]
  private val readMetrics: ShuffleReadMetrics =
    FieldUtils.readField(reader, "readMetrics", true).asInstanceOf[ShuffleReadMetrics]

  override protected def readBlocks(): Iterator[InputStream] = {
    logInfo(
      s"Shuffle read started: " +
        s"appId=$appId" +
        s", shuffleId=$shuffleId" +
        s", taskId=$taskId" +
        s", partitions: [$startPartition, $endPartition)" +
        s", maps: [$startMapIndex, $endMapIndex)")

    val inputStream =
      new UniffleInputStream(
        new MultiPartitionIterator[K, C](),
        shuffleId,
        startPartition,
        endPartition)
    Iterator.single(inputStream)
  }

  private class MultiPartitionIterator[K, C] extends AbstractIterator[Product2[K, C]] {
    private var iterators: util.Iterator[RssShuffleDataIterator[K, C]] = null
    private var currentIterator: RssShuffleDataIterator[K, C] = null

    if (numMaps > 0) {
      val shuffleDataIterList: util.List[RssShuffleDataIterator[K, C]] =
        new util.ArrayList[RssShuffleDataIterator[K, C]]()

      val emptyPartitionIds = new util.ArrayList[Int]
      for (partition <- startPartition until endPartition) {
        if (partitionToExpectBlocks.get(partition).isEmpty) {
          logInfo(s"$partition is a empty partition")
          emptyPartitionIds.add(partition)
        } else {
          val shuffleServerInfoList: util.List[ShuffleServerInfo] =
            partitionToShuffleServers.get(partition)
          // This mechanism of expectedTaskIdsBitmap filter is to filter out the most of data.
          // especially for AQE skew optimization
          val expectedTaskIdsBitmapFilterEnable: Boolean =
            !(mapStartIndex == 0 && mapEndIndex == Integer.MAX_VALUE) || shuffleServerInfoList.size > 1
          val retryMax: Int = rssConf.getInteger(
            RssClientConfig.RSS_CLIENT_RETRY_MAX,
            RssClientConfig.RSS_CLIENT_RETRY_MAX_DEFAULT_VALUE)
          val retryIntervalMax: Long = rssConf.getLong(
            RssClientConfig.RSS_CLIENT_RETRY_INTERVAL_MAX,
            RssClientConfig.RSS_CLIENT_RETRY_INTERVAL_MAX_DEFAULT_VALUE)
          val shuffleReadClient: ShuffleReadClient =
            ShuffleClientFactory.getInstance.createShuffleReadClient(
              ShuffleClientFactory.newReadBuilder
                .appId(appId)
                .shuffleId(shuffleId)
                .partitionId(partition)
                .basePath(basePath)
                .partitionNumPerRange(1)
                .partitionNum(partitionNum)
                .blockIdBitmap(partitionToExpectBlocks.get(partition))
                .taskIdBitmap(taskIdBitmap)
                .shuffleServerInfoList(shuffleServerInfoList)
                .hadoopConf(hadoopConf)
                .shuffleDataDistributionType(dataDistributionType)
                .expectedTaskIdsBitmapFilterEnable(expectedTaskIdsBitmapFilterEnable)
                .retryMax(retryMax)
                .retryIntervalMax(retryIntervalMax)
                .rssConf(rssConf))
          val iterator: RssShuffleDataIterWrapper[K, C] =
            new RssShuffleDataIterWrapper[K, C](
              dependency,
              shuffleReadClient,
              readMetrics,
              rssConf)
          shuffleDataIterList.add(iterator)
        }
      }
      if (!emptyPartitionIds.isEmpty) {
        logInfo(s"")
      }
      iterators = shuffleDataIterList.iterator()
      if (iterators.hasNext) {
        currentIterator = iterators.next
        iterators.remove()
      }
      context.addTaskCompletionListener(new TaskCompletionListener {
        override def onTaskCompletion(context: TaskContext): Unit = {
          context.taskMetrics.mergeShuffleReadMetrics()
          if (currentIterator != null) {
            currentIterator.cleanup()
          }
          if (iterators != null) {
            while (iterators.hasNext) {
              iterators.next().cleanup()
            }
          }
        }
      })
    }

    override def hasNext: Boolean = try {
      if (currentIterator == null) return false
      while (!currentIterator.hasNext) {
        currentIterator.cleanup()
        if (!iterators.hasNext) {
          currentIterator = null
          return false
        }
        currentIterator = iterators.next
        iterators.remove()
      }
      currentIterator.hasNext
    } catch {
      case e: RssException =>
        throw e
    }

    override def next: Product2[K, C] = {
      val result: Product2[K, C] = currentIterator.next
      result
    }
  }

  private class UniffleInputStream(
      iterator: MultiPartitionIterator[_, _],
      shuffleId: Int,
      startPartition: Int,
      endPartition: Int)
      extends java.io.InputStream {
    private var byteBuffer: ByteBuffer = null

    override def read(): Int = {
      while (byteBuffer == null || !byteBuffer.hasRemaining()) {
        if (!toNextBuffer()) {
          return -1
        }
      }
      byteBuffer.get() & 255
    }

    private def toNextBuffer(): Boolean = {
      if (byteBuffer != null && byteBuffer.hasRemaining) {
        return true
      }
      if (!iterator.hasNext) {
        return false
      }
      val next = iterator.next()
      if (next == null || next._2 == null) {
        return false
      }
      this.byteBuffer = next._2.asInstanceOf[ByteBuffer]
      true
    }

    override def read(arrayBytes: Array[Byte], off: Int, len: Int): Int = {
      require(arrayBytes != null, "Propagated buffer must not be null")
      require(off >= 0 && len >= 0 && len <= arrayBytes.length - off, "Invalid offset or length")

      if (len == 0) {
        return 0
      }

      var readBytes = 0
      while (readBytes < len) {
        while (byteBuffer == null || !byteBuffer.hasRemaining()) {
          if (!this.toNextBuffer)
            return if (readBytes > 0) readBytes else -1
        }
        val bytesToRead = Math.min(byteBuffer.remaining(), len - readBytes)
        byteBuffer.get(arrayBytes, off + readBytes, bytesToRead)
        readBytes += bytesToRead
      }
      readBytes
    }
  }
}

class RssShuffleDataIterWrapper[K, V](
    dependency: ShuffleDependency[_, _, _],
    readClient: ShuffleReadClient,
    shuffleReadMetrics: ShuffleReadMetrics,
    rssConf: RssConf)
    extends RssShuffleDataIterator[K, V](
      dependency.serializer,
      readClient,
      shuffleReadMetrics,
      rssConf) {

  override def createKVIterator(data: ByteBuffer): Iterator[Tuple2[AnyRef, AnyRef]] = {
    val element = Tuple2.apply(data.remaining().asInstanceOf[AnyRef], data.asInstanceOf[AnyRef])
    scala.Iterator.single(element)
  }
}
