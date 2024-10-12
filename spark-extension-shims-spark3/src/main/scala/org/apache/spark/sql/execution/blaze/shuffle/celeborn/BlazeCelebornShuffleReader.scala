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
package org.apache.spark.sql.execution.blaze.shuffle.celeborn

import java.io.InputStream
import java.io.IOException
import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.client.read.CelebornInputStream
import org.apache.celeborn.client.read.MetricsCallback
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.exception.CelebornIOException
import org.apache.celeborn.common.exception.PartitionUnRetryAbleException
import org.apache.celeborn.common.network.client.TransportClient
import org.apache.celeborn.common.network.protocol.TransportMessage
import org.apache.celeborn.common.protocol.MessageType
import org.apache.celeborn.common.protocol.PartitionLocation
import org.apache.celeborn.common.protocol.PbOpenStreamList
import org.apache.celeborn.common.protocol.PbOpenStreamListResponse
import org.apache.celeborn.common.protocol.PbStreamHandler
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.util.JavaUtils
import org.apache.celeborn.common.util.ThreadUtils
import org.apache.celeborn.common.util.Utils
import org.apache.celeborn.common.util.ExceptionMaker
import org.apache.spark.sql.execution.blaze.shuffle.BlazeRssShuffleReaderBase
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.celeborn.CelebornShuffleHandle
import org.apache.spark.shuffle.ShuffleReadMetricsReporter
import org.apache.spark.shuffle.celeborn.CelebornShuffleReader
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.shuffle.celeborn.CelebornShuffleReader.streamCreatorPool
import org.apache.spark.shuffle.celeborn.ExecutorShuffleIdTracker
import org.apache.spark.shuffle.celeborn.SparkUtils
import org.apache.spark.storage.BlockId
import org.apache.spark.util.CompletionIterator

class BlazeCelebornShuffleReader[K, C](
    conf: CelebornConf,
    handle: CelebornShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    startMapIndex: Option[Int] = None,
    endMapIndex: Option[Int] = None,
    context: TaskContext,
    metrics: ShuffleReadMetricsReporter,
    shuffleIdTracker: ExecutorShuffleIdTracker)
    extends BlazeRssShuffleReaderBase[K, C](handle, context)
    with Logging {

  private val shuffleClient = ShuffleClient.get(
    handle.appUniqueId,
    handle.lifecycleManagerHost,
    handle.lifecycleManagerPort,
    conf,
    handle.userIdentifier,
    handle.extension)

  private val exceptionRef = new AtomicReference[IOException]
  private val throwsFetchFailure = handle.throwsFetchFailure
  private val encodedAttemptId = BlazeCelebornShuffleManager.getEncodedAttemptNumber(context)

  override protected def readBlocks(): Iterator[(BlockId, InputStream)] = {

    val shuffleId = SparkUtils.celebornShuffleId(shuffleClient, handle, context, false)
    shuffleIdTracker.track(handle.shuffleId, shuffleId)
    logDebug(
      s"get shuffleId $shuffleId for appShuffleId ${handle.shuffleId} attemptNum ${context.stageAttemptNumber()}")

    // Update the context task metrics for each record read.
    val metricsCallback = new MetricsCallback {
      override def incBytesRead(bytesWritten: Long): Unit = {
        metrics.incRemoteBytesRead(bytesWritten)
        metrics.incRemoteBlocksFetched(1)
      }

      override def incReadTime(time: Long): Unit =
        metrics.incFetchWaitTime(time)
    }

    if (streamCreatorPool == null) {
      CelebornShuffleReader.synchronized {
        if (streamCreatorPool == null) {
          streamCreatorPool = ThreadUtils.newDaemonCachedThreadPool(
            "celeborn-create-stream-thread",
            conf.readStreamCreatorPoolThreads,
            60)
        }
      }
    }

    val startTime = System.currentTimeMillis()
    val fetchTimeoutMs = conf.clientFetchTimeoutMs
    val localFetchEnabled = conf.enableReadLocalShuffleFile
    val localHostAddress = Utils.localHostName(conf)
    val shuffleKey = Utils.makeShuffleKey(handle.appUniqueId, shuffleId)
    // startPartition is irrelevant
    val fileGroups = shuffleClient.updateFileGroup(shuffleId, startPartition)
    // host-port -> (TransportClient, PartitionLocation Array, PbOpenStreamList)
    val workerRequestMap = new util.HashMap[
      String,
      (TransportClient, util.ArrayList[PartitionLocation], PbOpenStreamList.Builder)]()

    var partCnt = 0

    (startPartition until endPartition).foreach { partitionId =>
      if (fileGroups.partitionGroups.containsKey(partitionId)) {
        fileGroups.partitionGroups.get(partitionId).asScala.foreach { location =>
          partCnt += 1
          val hostPort = location.hostAndFetchPort
          if (!workerRequestMap.containsKey(hostPort)) {
            val client = shuffleClient
              .getDataClientFactory()
              .createClient(location.getHost, location.getFetchPort)
            val pbOpenStreamList = PbOpenStreamList.newBuilder()
            pbOpenStreamList.setShuffleKey(shuffleKey)
            workerRequestMap
              .put(hostPort, (client, new util.ArrayList[PartitionLocation], pbOpenStreamList))
          }
          val (_, locArr, pbOpenStreamListBuilder) = workerRequestMap.get(hostPort)

          locArr.add(location)
          pbOpenStreamListBuilder
            .addFileName(location.getFileName)
            .addStartIndex(startMapIndex.getOrElse(0))
            .addEndIndex(endMapIndex.getOrElse(Int.MaxValue))
          pbOpenStreamListBuilder.addReadLocalShuffle(
            localFetchEnabled && location.getHost.equals(localHostAddress))
        }
      }
    }

    val locationStreamHandlerMap: ConcurrentHashMap[PartitionLocation, PbStreamHandler] =
      JavaUtils.newConcurrentHashMap()

    val futures = workerRequestMap
      .values()
      .asScala
      .map { entry =>
        streamCreatorPool.submit(new Runnable {
          override def run(): Unit = {
            val (client, locArr, pbOpenStreamListBuilder) = entry
            val msg = new TransportMessage(
              MessageType.BATCH_OPEN_STREAM,
              pbOpenStreamListBuilder.build().toByteArray)
            val pbOpenStreamListResponse =
              try {
                val response = client.sendRpcSync(msg.toByteBuffer, fetchTimeoutMs)
                TransportMessage
                  .fromByteBuffer(response)
                  .getParsedPayload[PbOpenStreamListResponse]
              } catch {
                case _: Exception => null
              }
            if (pbOpenStreamListResponse != null) {
              0 until locArr.size() foreach { idx =>
                val streamHandlerOpt = pbOpenStreamListResponse.getStreamHandlerOptList.get(idx)
                if (streamHandlerOpt.getStatus == StatusCode.SUCCESS.getValue) {
                  locationStreamHandlerMap.put(locArr.get(idx), streamHandlerOpt.getStreamHandler)
                }
              }
            }
          }
        })
      }
      .toList
    // wait for all futures to complete
    futures.foreach(f => f.get())
    val end = System.currentTimeMillis()
    logInfo(s"BatchOpenStream for $partCnt cost ${end - startTime}ms")

    val streams = JavaUtils.newConcurrentHashMap[Integer, CelebornInputStream]()

    def createInputStream(partitionId: Int): Unit = {
      val locations =
        if (fileGroups.partitionGroups.containsKey(partitionId)) {
          new util.ArrayList(fileGroups.partitionGroups.get(partitionId))
        } else new util.ArrayList[PartitionLocation]()
      val streamHandlers =
        if (locations != null) {
          val streamHandlerArr = new util.ArrayList[PbStreamHandler](locations.size())
          locations.asScala.foreach { loc =>
            streamHandlerArr.add(locationStreamHandlerMap.get(loc))
          }
          streamHandlerArr
        } else null
      if (exceptionRef.get() == null) {
        try {
          val inputStream = shuffleClient.readPartition(
            shuffleId,
            handle.shuffleId,
            partitionId,
            encodedAttemptId,
            startMapIndex.getOrElse(0),
            endMapIndex.getOrElse(Int.MaxValue),
            if (throwsFetchFailure) {
              new ExceptionMaker() {
                override def makeFetchFailureException(
                    appShuffleId: Int,
                    shuffleId: Int,
                    partitionId: Int,
                    e: Exception): Exception = new FetchFailedException(
                  null,
                  appShuffleId,
                  -1,
                  -1,
                  partitionId,
                  s"Celeborn FetchFailure with shuffle id $appShuffleId/$shuffleId",
                  e)
              }
            } else {
              null
            },
            locations,
            streamHandlers,
            fileGroups.mapAttempts,
            metricsCallback)
          streams.put(partitionId, inputStream)
        } catch {
          case e: IOException =>
            logError(s"Exception caught when readPartition $partitionId!", e)
            exceptionRef.compareAndSet(null, e)
          case e: Throwable =>
            logError(s"Non IOException caught when readPartition $partitionId!", e)
            exceptionRef.compareAndSet(null, new CelebornIOException(e))
        }
      }
    }

    val inputStreamCreationWindow = conf.clientInputStreamCreationWindow
    (startPartition until Math.min(startPartition + inputStreamCreationWindow, endPartition))
      .foreach(partitionId => {
        streamCreatorPool.submit(new Runnable {
          override def run(): Unit = {
            createInputStream(partitionId)
          }
        })
      })

    val recordIter = (startPartition until endPartition).iterator
      .map(partitionId => {
        if (handle.numMappers > 0) {
          val startFetchWait = System.nanoTime()
          var inputStream: CelebornInputStream = streams.get(partitionId)
          while (inputStream == null) {
            if (exceptionRef.get() != null) {
              exceptionRef.get() match {
                case ce @ (_: CelebornIOException | _: PartitionUnRetryAbleException) =>
                  if (throwsFetchFailure &&
                    shuffleClient.reportShuffleFetchFailure(handle.shuffleId, shuffleId)) {
                    throw new FetchFailedException(
                      null,
                      handle.shuffleId,
                      -1,
                      -1,
                      partitionId,
                      SparkUtils.FETCH_FAILURE_ERROR_MSG + handle.shuffleId + "/" + shuffleId,
                      ce)
                  } else
                    throw ce
                case e => throw e
              }
            }
            log.info("inputStream is null, sleeping...")
            Thread.sleep(50)
            inputStream = streams.get(partitionId)
          }
          metricsCallback.incReadTime(
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startFetchWait))
          // ensure inputStream is closed when task completes
          context.addTaskCompletionListener[Unit](_ => inputStream.close())

          // Advance the input creation window
          if (partitionId + inputStreamCreationWindow < endPartition) {
            streamCreatorPool.submit(new Runnable {
              override def run(): Unit = {
                createInputStream(partitionId + inputStreamCreationWindow)
              }
            })
          }

          (partitionId, inputStream)
        } else {
          (partitionId, CelebornInputStream.empty())
        }
      })
      .filter { case (_, inputStream) =>
        inputStream != CelebornInputStream.empty()
      }

    CompletionIterator[(BlockId, InputStream), Iterator[(BlockId, InputStream)]](
      recordIter.map(block => (null, block._2)), // blockId is not used
      () => context.taskMetrics().mergeShuffleReadMetrics())
  }
}
