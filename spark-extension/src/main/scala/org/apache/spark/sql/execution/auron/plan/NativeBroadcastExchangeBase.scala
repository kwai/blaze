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
package org.apache.spark.sql.execution.auron.plan

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.Future
import java.util.concurrent.TimeoutException
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.collection.immutable.SortedMap
import scala.concurrent.Promise

import org.apache.commons.lang3.reflect.MethodUtils
import org.apache.spark.OneToOneDependency
import org.apache.spark.Partition
import org.apache.spark.SparkException
import org.apache.spark.TaskContext
import org.apache.spark.broadcast
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.auron.JniBridge
import org.apache.spark.sql.auron.MetricNode
import org.apache.spark.sql.auron.NativeConverters
import org.apache.spark.sql.auron.NativeHelper
import org.apache.spark.sql.auron.NativeRDD
import org.apache.spark.sql.auron.NativeSupports
import org.apache.spark.sql.auron.Shims
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.InterpretedUnsafeProjection
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.catalyst.plans.physical.BroadcastPartitioning
import org.apache.spark.sql.catalyst.plans.physical.IdentityBroadcastMode
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.auron.plan.NativeBroadcastExchangeBase.buildBroadcastData
import org.apache.spark.sql.execution.auron.shuffle.BlockObject
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.exchange.BroadcastExchangeLike
import org.apache.spark.sql.execution.joins.HashedRelationBroadcastMode
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types.BinaryType

import org.apache.auron.{protobuf => pb}

abstract class NativeBroadcastExchangeBase(mode: BroadcastMode, override val child: SparkPlan)
    extends BroadcastExchangeLike
    with NativeSupports {

  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = BroadcastPartitioning(mode)

  def broadcastMode: BroadcastMode = this.mode

  protected val hashMapOutput: Seq[Attribute] = output
    .map(_.withNullability(true)) :+ AttributeReference("~TABLE", BinaryType, nullable = true)()

  protected val nativeSchema: pb.Schema = Util.getNativeSchema(output)
  protected val nativeHashMapSchema: pb.Schema = Util.getNativeSchema(hashMapOutput)

  def getRunId: UUID
  override lazy val metrics: Map[String, SQLMetric] = SortedMap[String, SQLMetric]() ++ Map(
    NativeHelper
      .getDefaultNativeMetrics(sparkContext)
      .toSeq :+
      ("dataSize", SQLMetrics.createSizeMetric(sparkContext, "data size")) :+
      ("numOutputRows", SQLMetrics.createMetric(sparkContext, "number of output rows")) :+
      ("collectTime", SQLMetrics.createTimingMetric(sparkContext, "time to collect")) :+
      ("buildTime", SQLMetrics.createTimingMetric(sparkContext, "time to build")) :+
      ("broadcastTime", SQLMetrics.createTimingMetric(sparkContext, "time to broadcast")): _*)

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "BroadcastExchange does not support the execute() code path.")
  }

  override def doPrepare(): Unit = {
    // Materialize the future.
    relationFuture
  }

  override def doExecuteBroadcast[T](): Broadcast[T] = {
    val singlePartition = new Partition() {
      override def index: Int = 0
    }
    val broadcastReadNativePlan = doExecuteNative().nativePlan(singlePartition, null)
    val rowsIter = NativeHelper.executeNativePlan(
      broadcastReadNativePlan,
      MetricNode(Map(), Nil, None),
      singlePartition,
      None)
    val pruneKeyField = new InterpretedUnsafeProjection(
      output.zipWithIndex
        .map(v => BoundReference(v._2, v._1.dataType, v._1.nullable))
        .toArray)

    val dataRows = rowsIter
      .map(pruneKeyField)
      .map(_.copy())
      .toArray

    val broadcast = relationFuture.get // broadcast must be resolved
    val v = mode.transform(dataRows)
    val dummyBroadcasted = new Broadcast[Any](-1) {
      override protected def getValue(): Any = v
      override protected def doUnpersist(blocking: Boolean): Unit = {
        MethodUtils.invokeMethod(broadcast, true, "doUnpersist", Array(blocking))
      }
      override protected def doDestroy(blocking: Boolean): Unit = {
        MethodUtils.invokeMethod(broadcast, true, "doDestroy", Array(blocking))
      }
    }
    dummyBroadcasted.asInstanceOf[Broadcast[T]]
  }

  def doExecuteBroadcastNative[T](): broadcast.Broadcast[T] = {
    val conf = SparkSession.getActiveSession.map(_.sqlContext.conf).orNull
    val timeout: Long = conf.broadcastTimeout
    try {
      relationFuture.get(timeout, TimeUnit.SECONDS).asInstanceOf[broadcast.Broadcast[T]]
    } catch {
      case ex: TimeoutException =>
        logError(s"Could not execute broadcast in $timeout secs.", ex)
        if (!relationFuture.isDone) {
          sparkContext.cancelJobGroup(getRunId.toString)
          relationFuture.cancel(true)
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
      None,
      Nil,
      rddShuffleReadFull = true,
      (_, _) => {
        val resourceId = s"ArrowBroadcastExchangeExec:${UUID.randomUUID()}"
        val provideIpcIterator = () => {
          broadcast.value.iterator.map(bytes =>
            new BlockObject {
              override def hasByteBuffer: Boolean = true
              override def getByteBuffer: ByteBuffer = ByteBuffer.wrap(bytes)
              override def close(): Unit = {}
            })
        }

        JniBridge.resourcesMap.put(resourceId, () => provideIpcIterator())
        pb.PhysicalPlanNode
          .newBuilder()
          .setIpcReader(
            pb.IpcReaderExecNode
              .newBuilder()
              .setSchema(nativeHashMapSchema)
              .setNumPartitions(1)
              .setIpcProviderResourceId(resourceId)
              .build())
          .build()
      },
      friendlyName = "NativeRDD.BroadcastRead")
  }

  def collectNative(): Array[Array[Byte]] = {
    val inputRDD = NativeHelper.executeNative(child match {
      case child if NativeHelper.isNative(child) => child
      case child => Shims.get.createConvertToNativeExec(child)
    })
    val modifiedMetrics = metrics ++ Map("output_rows" -> metrics("numOutputRows"))
    val nativeMetrics = MetricNode(modifiedMetrics, inputRDD.metrics :: Nil)

    val ipcRDD =
      new RDD[Array[Byte]](sparkContext, new OneToOneDependency(inputRDD) :: Nil) {
        setName("NativeRDD.BroadcastWrite")
        Shims.get.setRDDShuffleReadFull(this, inputRDD.isShuffleReadFull)

        override protected def getPartitions: Array[Partition] = inputRDD.partitions

        override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {
          val resourceId = s"ArrowBroadcastExchangeExec.input:${UUID.randomUUID()}"
          val bos = new ByteArrayOutputStream()
          JniBridge.resourcesMap.put(
            resourceId,
            (byteBuffer: ByteBuffer) => {
              val byteArray = new Array[Byte](byteBuffer.capacity())
              byteBuffer.get(byteArray)
              bos.write(byteArray)
              metrics("dataSize") += byteArray.length
            })

          val input = inputRDD.nativePlan(inputRDD.partitions(split.index), context)
          val nativeIpcWriterExec = pb.PhysicalPlanNode
            .newBuilder()
            .setIpcWriter(
              pb.IpcWriterExecNode
                .newBuilder()
                .setInput(input)
                .setIpcConsumerResourceId(resourceId)
                .build())
            .build()

          // execute ipc writer and fill output channels
          val iter =
            NativeHelper.executeNativePlan(
              nativeIpcWriterExec,
              nativeMetrics,
              split,
              Some(context))
          assert(iter.isEmpty)

          // return ipcs as iterator
          Iterator.single(bos.toByteArray)
        }
      }

    // build broadcast data
    val collectedData = ipcRDD.collect()
    val keys = mode match {
      case mode: HashedRelationBroadcastMode => mode.key
      case IdentityBroadcastMode => Nil
    }
    buildBroadcastData(collectedData, keys, nativeSchema)
  }

  @transient
  lazy val relationFuturePromise: Promise[Broadcast[Any]] = Promise[Broadcast[Any]]()

  @transient
  lazy val relationFuture: Future[Broadcast[Any]] = {
    SQLExecution.withThreadLocalCaptured[Broadcast[Any]](
      Shims.get.getSqlContext(this).sparkSession,
      BroadcastExchangeExec.executionContext) {
      try {
        sparkContext.setJobGroup(
          getRunId.toString,
          s"native broadcast exchange (runId $getRunId)",
          interruptOnCancel = true)
        val broadcasted = sparkContext.broadcast(collectNative().asInstanceOf[Any])
        relationFuturePromise.trySuccess(broadcasted)
        broadcasted
      } catch {
        case e: Throwable =>
          relationFuturePromise.tryFailure(e)
          throw e
      }
    }
  }

  override protected def doCanonicalize(): SparkPlan =
    Shims.get.createNativeBroadcastExchangeExec(mode.canonicalized, child.canonicalized)
}

object NativeBroadcastExchangeBase {

  def nativeExecutionTag: TreeNodeTag[Boolean] = TreeNodeTag("arrowBroadcastNativeExecution")

  def buildBroadcastData(
      collectedData: Array[Array[Byte]],
      keys: Seq[Expression],
      nativeSchema: pb.Schema): Array[Array[Byte]] = {

    val readerIpcProviderResourceId = s"BuildBroadcastDataReader:${UUID.randomUUID()}"
    val readerExec = pb.IpcReaderExecNode
      .newBuilder()
      .setSchema(nativeSchema)
      .setIpcProviderResourceId(readerIpcProviderResourceId)

    val buildHashMapExec = pb.BroadcastJoinBuildHashMapExecNode
      .newBuilder()
      .setInput(pb.PhysicalPlanNode.newBuilder().setIpcReader(readerExec))
      .addAllKeys(keys.map(key => NativeConverters.convertExpr(key)).asJava)

    val writerIpcProviderResourceId = s"BuildBroadcastDataWriter:${UUID.randomUUID()}"
    val writerExec = pb.IpcWriterExecNode
      .newBuilder()
      .setInput(pb.PhysicalPlanNode.newBuilder().setBroadcastJoinBuildHashMap(buildHashMapExec))
      .setIpcConsumerResourceId(writerIpcProviderResourceId)

    val exec = pb.PhysicalPlanNode
      .newBuilder()
      .setIpcWriter(writerExec)
      .build()

    // input
    val provideIpcIterator = () => {
      collectedData.iterator.map(bytes =>
        new BlockObject {
          override def hasByteBuffer: Boolean = true
          override def getByteBuffer: ByteBuffer = ByteBuffer.wrap(bytes)
          override def close(): Unit = {}
        })
    }
    JniBridge.resourcesMap.put(readerIpcProviderResourceId, () => provideIpcIterator())

    // output
    val bos = new ByteArrayOutputStream()
    val consumeIpc = (byteBuffer: ByteBuffer) => {
      val byteArray = new Array[Byte](byteBuffer.capacity())
      byteBuffer.get(byteArray)
      bos.write(byteArray)
    }
    JniBridge.resourcesMap.put(writerIpcProviderResourceId, consumeIpc)

    // execute
    val singlePartition = new Partition {
      override def index: Int = 0
    }
    assert(NativeHelper.executeNativePlan(exec, null, singlePartition, None).isEmpty)
    Array(bos.toByteArray)
  }
}
