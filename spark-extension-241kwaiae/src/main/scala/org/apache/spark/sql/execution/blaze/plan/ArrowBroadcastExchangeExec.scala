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

package org.apache.spark.sql.execution.blaze.plan

import java.io.ByteArrayInputStream
import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.Future
import java.util.concurrent.TimeoutException
import java.util.concurrent.TimeUnit

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Promise

import org.apache.spark.InterruptibleIterator
import org.apache.spark.Partition
import org.apache.spark.SparkException
import org.apache.spark.TaskContext
import org.apache.spark.broadcast
import org.apache.spark.OneToOneDependency

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.blaze.JniBridge
import org.apache.spark.sql.blaze.MetricNode
import org.apache.spark.sql.blaze.NativeHelper
import org.apache.spark.sql.blaze.NativeRDD
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.blaze.arrowio.IpcInputStreamIterator
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.physical.BroadcastPartitioning

case class ArrowBroadcastExchangeExec(mode: BroadcastMode, override val child: SparkPlan)
    extends ArrowBroadcastExchangeBase {

  override lazy val runId: UUID = UUID.randomUUID()

  override lazy val metrics: Map[String, SQLMetric] = Map(
    NativeHelper
      .getDefaultNativeMetrics(sparkContext)
      .filterKeys(Set("output_rows", "elapsed_compute"))
      .toSeq :+
      ("dataSize", SQLMetrics.createSizeMetric(sparkContext, "data size")) :+
      ("collectTime", SQLMetrics.createTimingMetric(sparkContext, "time to collect")) :+
      ("buildTime", SQLMetrics.createTimingMetric(sparkContext, "time to build")) :+
      ("broadcastTime", SQLMetrics.createTimingMetric(sparkContext, "time to broadcast")) :+
      ("ipc_write_rows", SQLMetrics.createMetric(sparkContext, "Native.ipc_write_rows")) :+
      ("ipc_write_time", SQLMetrics
        .createNanoTimingMetric(sparkContext, "Native.ipc_write_time")): _*)

  override def outputPartitioning: Partitioning = BroadcastPartitioning(mode)

  override def doCanonicalize(): SparkPlan = child.canonicalized

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "BroadcastExchange does not support the execute() code path.")
  }

  override def doPrepare(): Unit = {
    // Materialize the future.
    relationFuture
  }

  override def doExecuteBroadcast[T](): Broadcast[T] = {
    val broadcast = nonNativeBroadcastExec.executeBroadcast[T]()
    for ((k, metric) <- nonNativeBroadcastExec.metrics) {
      metrics.get(k).foreach(_.add(metric.value))
    }
    broadcast
  }

  def doExecuteBroadcastNative[T](): broadcast.Broadcast[T] = {
    val conf = SparkSession.getActiveSession.map(_.sqlContext.conf).orNull
    val timeout: Long = conf.broadcastTimeout
    try {
      nativeRelationFuture.get(timeout, TimeUnit.SECONDS).asInstanceOf[broadcast.Broadcast[T]]
    } catch {
      case ex: TimeoutException =>
        logError(s"Could not execute broadcast in $timeout secs.", ex)
        if (!nativeRelationFuture.isDone) {
          sparkContext.cancelJobGroup(runId.toString)
          nativeRelationFuture.cancel(true)
        }
        throw new SparkException("Native broadcast exchange timed out.", ex)
    }
  }

  override def doExecuteNative(): NativeRDD = {
    val broadcast = doExecuteBroadcastNative[Array[Array[Byte]]]()
    val nativeMetrics = MetricNode(metrics, Nil)
    val partitions = Array(new Partition() {
      override def index: Int = 0
    })

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      partitions,
      Nil,
      rddShuffleReadFull = true,
      (_, context) => {
        val resourceId = s"ArrowBroadcastExchangeExec:${UUID.randomUUID()}"
        val provideIpcIterator = () => {
          val ipcIterator = broadcast.value.iterator.flatMap { bytes =>
            val inputStream = new ByteArrayInputStream(bytes)
            IpcInputStreamIterator(inputStream, decompressingNeeded = false, context)
          }
          new InterruptibleIterator(context, ipcIterator)
        }
        JniBridge.resourcesMap.put(resourceId, () => provideIpcIterator())
        PhysicalPlanNode
          .newBuilder()
          .setIpcReader(
            IpcReaderExecNode
              .newBuilder()
              .setSchema(nativeSchema)
              .setNumPartitions(1)
              .setIpcProviderResourceId(resourceId)
              .setMode(IpcReadMode.CHANNEL)
              .build())
          .build()
      },
      friendlyName = "NativeRDD.BroadcastRead")
  }

  val nativeSchema: Schema = Util.getNativeSchema(output)

  def collectNative(): Array[Array[Byte]] = {
    val inputRDD = NativeHelper.executeNative(child match {
      case child if NativeHelper.isNative(child) => child
      case child => ConvertToNativeExec(child)
    })
    val modifiedMetrics = metrics ++ Map(
      "output_rows" -> metrics("ipc_write_rows"),
      "elapsed_compute" -> metrics("ipc_write_time"))
    val nativeMetrics = MetricNode(modifiedMetrics, inputRDD.metrics :: Nil)

    val ipcRDD = new RDD[Array[Byte]](sparkContext, new OneToOneDependency(inputRDD) :: Nil) {
      setName("NativeRDD.BroadcastWrite")

      override protected def getPartitions: Array[Partition] = inputRDD.partitions

      override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {
        val resourceId = s"ArrowBroadcastExchangeExec.input:${UUID.randomUUID()}"
        val ipcs = ArrayBuffer[Array[Byte]]()
        JniBridge.resourcesMap.put(
          resourceId,
          (byteBuffer: ByteBuffer) => {
            val byteArray = new Array[Byte](byteBuffer.capacity())
            byteBuffer.get(byteArray)
            ipcs += byteArray
            metrics("dataSize") += byteArray.length
          })

        val input = inputRDD.nativePlan(inputRDD.partitions(split.index), context)
        val nativeIpcWriterExec = PhysicalPlanNode
          .newBuilder()
          .setIpcWriter(
            IpcWriterExecNode
              .newBuilder()
              .setInput(input)
              .setIpcConsumerResourceId(resourceId)
              .build())
          .build()

        // execute ipc writer and fill output channels
        val iter =
          NativeHelper.executeNativePlan(nativeIpcWriterExec, nativeMetrics, split, context)
        assert(iter.isEmpty)

        // return ipcs as iterator
        ipcs.iterator
      }
    }
    ipcRDD.collect()
  }

  @transient
  private lazy val relationFuture: Future[Broadcast[Any]] = if (isNative) {
    nativeRelationFuture.asInstanceOf[Future[Broadcast[Any]]]
  } else {
    val relationFutureField = classOf[BroadcastExchangeExec].getDeclaredField("relationFuture")
    relationFutureField.setAccessible(true)
    relationFutureField.get(nonNativeBroadcastExec).asInstanceOf[Future[Broadcast[Any]]]
  }

  @transient
  private lazy val nonNativeBroadcastExec: BroadcastExchangeExec = {
    BroadcastExchangeExec(mode, ConvertToUnsafeRowExec(child))
  }

  @transient
  private lazy val nativeRelationFuture: Future[Broadcast[Array[Array[Byte]]]] = {
    SQLExecution.withThreadLocalCaptured[Broadcast[Array[Array[Byte]]]](
      sqlContext.sparkSession,
      BroadcastExchangeExec.executionContext) {
      try {
        sparkContext.setJobGroup(
          runId.toString,
          s"native broadcast exchange (runId $runId)",
          interruptOnCancel = true)
        val broadcasted = sparkContext.broadcast(collectNative())
        Promise[Broadcast[Array[Array[Byte]]]].trySuccess(broadcasted)
        broadcasted
      } catch {
        case e: Throwable =>
          Promise[Broadcast[Array[Array[Byte]]]].tryFailure(e)
          throw e
      }
    }
  }

  override def withNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
    copy(child = newChildren.head)
}
