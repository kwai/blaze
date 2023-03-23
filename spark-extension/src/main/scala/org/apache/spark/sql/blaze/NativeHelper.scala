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

package org.apache.spark.sql.blaze

import java.io.File
import java.io.FileNotFoundException
import java.io.IOException
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.SynchronousQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.immutable.TreeMap
import scala.concurrent.TimeoutException
import scala.language.reflectiveCalls

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.Partition
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkEnv
import org.apache.spark.SparkException
import org.apache.spark.TaskContext
import org.blaze.protobuf.PartitionId
import org.blaze.protobuf.PhysicalPlanNode
import org.blaze.protobuf.TaskDefinition

import org.apache.spark.internal.Logging
import org.apache.spark.internal.config
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.blaze.arrowio.ArrowFFIImportIterator
import org.apache.spark.sql.execution.blaze.arrowio.ColumnarHelper
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.DiskBlockManager

object NativeHelper extends Logging {
  val currentUser: UserGroupInformation = UserGroupInformation.getCurrentUser
  private val conf: SparkConf = SparkEnv.get.conf
  private val diskBlockManager: DiskBlockManager = SparkEnv.get.blockManager.diskBlockManager

  val batchSize: Int = conf.getInt("spark.blaze.batchSize", 10000)

  val nativeMemory: Long = {
    val MEMORY_OVERHEAD_FACTOR = 0.10
    val MEMORY_OVERHEAD_MIN = 384L
    val executorMemory = conf.get(config.EXECUTOR_MEMORY)
    val executorMemoryOverheadMiB = conf
      .get(config.EXECUTOR_MEMORY_OVERHEAD)
      .getOrElse(math.max((MEMORY_OVERHEAD_FACTOR * executorMemory).toLong, MEMORY_OVERHEAD_MIN))
    executorMemoryOverheadMiB * 1024L * 1024L
  }
  val memoryFraction: Double = conf.getDouble("spark.blaze.memoryFraction", 0.4);

  def isNative(exec: SparkPlan): Boolean =
    Shims.get.sparkPlanShims.isNative(exec)

  def getUnderlyingNativePlan(exec: SparkPlan): NativeSupports =
    Shims.get.sparkPlanShims.getUnderlyingNativePlan(exec)

  def executeNative(exec: SparkPlan): NativeRDD =
    Shims.get.sparkPlanShims.executeNative(exec)

  def executeNativePlan(
      nativePlan: PhysicalPlanNode,
      metrics: MetricNode,
      partition: Partition,
      context: TaskContext): Iterator[InternalRow] = {

    val wrapper = BlazeCallNativeWrapper(nativePlan, partition, context, metrics)
    new ArrowFFIImportIterator(wrapper, context).flatMap { batch =>
      ColumnarHelper.batchAsRowIter(batch)
    }
  }

  def executeNativePlanColumnar(
      nativePlan: PhysicalPlanNode,
      metrics: MetricNode,
      partition: Partition,
      context: TaskContext): Iterator[ColumnarBatch] = {

    val wrapper = BlazeCallNativeWrapper(nativePlan, partition, context, metrics)
    new ArrowFFIImportIterator(wrapper, context)
  }

  def getDefaultNativeMetrics(sc: SparkContext): Map[String, SQLMetric] =
    TreeMap(
      "output_rows" -> SQLMetrics.createMetric(sc, "Native.output_rows"),
      "output_batches" -> SQLMetrics.createMetric(sc, "Native.output_batches"),
      "input_rows" -> SQLMetrics.createMetric(sc, "Native.input_rows"),
      "input_batches" -> SQLMetrics.createMetric(sc, "Native.input_batches"),
      "elapsed_compute" -> SQLMetrics.createNanoTimingMetric(sc, "Native.elapsed_compute"),
      "join_time" -> SQLMetrics.createNanoTimingMetric(sc, "Native.join_time"),
      "spilled_bytes" -> SQLMetrics.createSizeMetric(sc, "Native.spilled_bytes"))
}

case class MetricNode(
    metrics: Map[String, SQLMetric],
    children: Seq[MetricNode],
    metricValueHandler: Option[(String, Long) => Unit] = None)
    extends Logging {

  def getChild(i: Int): MetricNode =
    children(i)

  def add(metricName: String, v: Long): Unit = {
    metrics.get(metricName).foreach(_.add(v))
    metricValueHandler.foreach(_.apply(metricName, v))
  }
}

case class BlazeCallNativeWrapper(
    nativePlan: PhysicalPlanNode,
    partition: Partition,
    context: TaskContext,
    metrics: MetricNode)
    extends Logging {

  private val valueQueue: SynchronousQueue[Object] = new SynchronousQueue()
  private val errorQueue: SynchronousQueue[Object] = new SynchronousQueue()
  private val finished: AtomicBoolean = new AtomicBoolean(false)
  private val nativeThreadFinished: BlockingQueue[Object] = new ArrayBlockingQueue(1)

  BlazeCallNativeWrapper.initNative()

  logInfo(s"Start executing native plan")
  JniBridge.callNative(this)

  def isFinished: Boolean = !JniBridge.isTaskRunning || finished.get()
  def finish(): Unit = {
    if (!isFinished) {
      finished.set(true)
      waitUntilNativeThreadFinished()
    }
  }

  protected def getMetrics: MetricNode = metrics

  protected def getRawTaskDefinition: Array[Byte] = {
    // do not use context.partitionId since it is not correct in Union plans.
    val partitionId: PartitionId = PartitionId
      .newBuilder()
      .setPartitionId(partition.index)
      .setStageId(context.stageId())
      .setJobId(partition.index.toString)
      .build()

    val taskDefinition = TaskDefinition
      .newBuilder()
      .setTaskId(partitionId)
      .setPlan(nativePlan)
      .build()
    taskDefinition.toByteArray
  }

  def nextBatch(schemaPtr: Long, arrayPtr: Long): Boolean = {
    while (!isFinished && { checkError(); true } && !enqueueWithTimeout((schemaPtr, arrayPtr))) {}
    while (!isFinished && { checkError(); true }) {
      dequeueWithTimeout() match {
        case java.lang.Boolean.TRUE =>
          return true

        case java.lang.Boolean.FALSE =>
          finish()
          return false

        case null =>
        // do nothing
      }
    }
    !isFinished
  }

  protected def enqueueWithTimeout(value: Object): Boolean = {
    valueQueue.offer(value, 100, TimeUnit.MILLISECONDS)
  }

  protected def dequeueWithTimeout(): Object = {
    valueQueue.poll(100, TimeUnit.MILLISECONDS)
  }

  protected def enqueueError(value: Object): Boolean = {
    errorQueue.offer(value, 100, TimeUnit.MILLISECONDS)
  }

  protected def checkError(): Unit = {
    errorQueue.poll() match {
      case e: Throwable =>
        finish()
        classOf[BlazeCallNativeWrapper].synchronized {
          TaskContext.setTaskContext(context)
          if (JniBridge.isTaskRunning) {
            throw new SparkException("blaze native execution error", e)
          }
        }
      case null =>
      // do nothing
    }
  }

  protected def finishNativeThread(): Unit = {
    nativeThreadFinished.offer(Some(true))
  }

  protected def waitUntilNativeThreadFinished(timeout: Long = 5000): Unit = {
    for (_ <- 0L until timeout / 100) {
      if (nativeThreadFinished.poll(100, TimeUnit.MILLISECONDS) != null) {
        return
      }
    }
    throw new TimeoutException("Timeout waiting blaze thread to be finished")
  }
}

object BlazeCallNativeWrapper extends Logging {
  def initNative(): Unit = {
    lazyInitNative
  }

  private lazy val lazyInitNative: Unit = {
    logInfo(
      "Initializing native environment (" +
        s"batchSize=${NativeHelper.batchSize}, " +
        s"nativeMemory=${NativeHelper.nativeMemory}, " +
        s"memoryFraction=${NativeHelper.memoryFraction}")

    BlazeCallNativeWrapper.load("blaze")
    JniBridge.initNative(
      NativeHelper.batchSize,
      NativeHelper.nativeMemory,
      NativeHelper.memoryFraction)
  }

  private def load(name: String): Unit = {
    val libraryToLoad = System.mapLibraryName(name)
    try {
      val temp =
        File.createTempFile("jnilib-", ".tmp", new File(System.getProperty("java.io.tmpdir")))
      val is = classOf[NativeSupports].getClassLoader.getResourceAsStream(libraryToLoad)
      try {
        if (is == null) {
          throw new FileNotFoundException(libraryToLoad)
        }
        Files.copy(is, temp.toPath, StandardCopyOption.REPLACE_EXISTING)
        System.load(temp.getAbsolutePath)
      } finally {
        if (is != null) {
          is.close()
        }
      }
    } catch {
      case e: IOException =>
        throw new IllegalStateException("error loading native libraries: " + e)
    }
  }

}
