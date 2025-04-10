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

import java.io.InputStream

import scala.collection.JavaConverters._

import com.kuaishou.dataarch.shuffle.common.io.UnifiedDataInputBuffer
import com.kuaishou.dataarch.shuffle.common.iterator.UnifiedRawKeyValueIterator
import com.kuaishou.dataarch.shuffle.common.reporter.Reporter
import com.kuaishou.dataarch.shuffle.proto.dto.common.PartitionStatistics
import com.kuaishou.dataarch.shuffle.proto.dto.enums.AppTypeEnum
import com.kuaishou.dataarch.shuffle.proto.dto.enums.ShuffleTypeEnum
import com.kuaishou.dataarch.shuffle.reader.model
import com.kuaishou.dataarch.shuffle.reader.service.UnifiedShuffleReader
import com.kuaishou.dataarch.shuffle.reader.service.UnifiedShuffleReader.Context
import org.apache.avro.util.ReusableByteArrayInputStream
import org.apache.spark.MapOutputTracker
import org.apache.spark.SparkEnv
import org.apache.spark.TaskContext

import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.stream.StreamShuffleHandle
import org.apache.spark.shuffle.ShuffleReadMetricsReporter
import org.apache.spark.storage.BlockId
import org.apache.spark.storage.BlockManager
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.Utils

class BlazeRssBlockStoreShuffleReader[K, C](
    handle: StreamShuffleHandle[K, C],
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    readMetrics: ShuffleReadMetricsReporter,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker,
    startMapId: Option[Int] = None,
    endMapId: Option[Int] = None,
    partitionStatistics: Option[Seq[PartitionStatistics]] = None,
    isLocalShuffleReader: Boolean = false)
    extends BlazeBlockStoreShuffleReaderBase[K, C](handle, context)
    with Logging {

  override def readBlocks(): Iterator[InputStream] = {

    val sessionInfo = handle.sessionInfo

    def resetStream(
        buffer: UnifiedDataInputBuffer,
        reuseStream: ReusableByteArrayInputStream): Unit = {
      reuseStream.setByteArray(buffer.getData, buffer.getPosition, buffer.getLength)
      readMetrics.incRemoteBytesRead(buffer.getLength)
    }

    lazy val localDirs = Utils.getConfiguredLocalDirs(SparkEnv.get.conf).mkString(",")

    val reporter = new ReaderMetricsReporter(readMetrics)

    val readContextBuilder = Context
      .builder[K, C]()
      .sessionId(sessionInfo.getSessionId)
      .sessionAttemptId(sessionInfo.getAttemptId)
      .stageId(handle.shuffleId)
      .taskId(context.partitionId())
      .appShuffleMasterId(sessionInfo.getAppShuffleMasterId)
      .taskAttemptId(context.attemptNumber())
      .codec(sessionInfo.getCompressClass)
      .startPartId(startPartition)
      .endPartId(endPartition)
      .startMapId(startMapId.getOrElse(0))
      .endMapId(endMapId.getOrElse(0))
      .reporter(reporter)
      .appType(AppTypeEnum.SPARK)
      .shuffleType(ShuffleTypeEnum.HASH)
      .localDirsStr(localDirs)
      .conf(Utils.getHadoopConf())
    partitionStatistics.foreach(it => readContextBuilder.partitionStatistics(it.asJava))

    val readContext = readContextBuilder.build()

    val realReader = UnifiedShuffleReader.getReader(readContext)

    var shuffleNextCalled: Long = 0

    val blockIter: Iterator[(BlockId, InputStream)] = new Iterator[(BlockId, InputStream)] {
      val start: Long = System.currentTimeMillis()
      val innerIterator: UnifiedRawKeyValueIterator = realReader.read()
      readMetrics.incBytesReadTime(System.currentTimeMillis() - start)
      val rbais = new ReusableByteArrayInputStream()
      var innerHashNext = false

      var nextCalled = false
      // call inner next in constructor for the first element
      innerNext()

      def innerNext(): Unit = {
        val start = System.currentTimeMillis()
        innerHashNext = innerIterator.next()
        shuffleNextCalled += 1
        readMetrics.incFetchWaitTime(System.currentTimeMillis() - start)
      }

      def getBlock: (BlockId, InputStream) = {
        resetStream(innerIterator.getValue, rbais)
        readMetrics.incRemoteBlocksFetched(1)
        (null, rbais)
      }

      override def hasNext: Boolean = {
        if (nextCalled) {
          innerNext()
          nextCalled = false
        }
        innerHashNext
      }

      override def next(): (BlockId, InputStream) = {
        nextCalled = true
        getBlock

      }
    }

    val succTaskIter =
      CompletionIterator[(BlockId, InputStream), Iterator[(BlockId, InputStream)]](
        blockIter, {
          logInfo(s"Closing stream shuffle reader in task ${context.taskAttemptId()} for now...")
          realReader.close()
          logInfo(s"Stream shuffle reader in task ${context.taskAttemptId()} closed.")
          context.taskMetrics().mergeShuffleReadMetrics(true)
        })
    succTaskIter.map(_._2)
  }

  class ReaderMetricsReporter(readMetrics: ShuffleReadMetricsReporter)
      extends Reporter[model.ShuffleStatus] {
    override def report(t: model.ShuffleStatus): Unit = {}
  }
}
