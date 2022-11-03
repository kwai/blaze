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
import java.util.UUID
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.SynchronousQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import scala.annotation.tailrec
import scala.collection.immutable.TreeMap
import scala.concurrent.TimeoutException
import scala.language.reflectiveCalls

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.ShuffledRDDPartition
import org.apache.spark.shuffle.ShuffleReader
import org.apache.spark.sql.blaze.kwai.BlazeOperatorMetricsCollector
import org.apache.spark.sql.blaze.kwai.BlazeOperatorMetricsCollector.isBlazeOperatorMetricsEnabled
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.blaze.arrowio.ArrowFFIImportIterator
import org.apache.spark.sql.execution.blaze.arrowio.ColumnarHelper
import org.apache.spark.sql.execution.blaze.shuffle.ArrowBlockStoreShuffleReader
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.metric.SQLShuffleReadMetricsReporter
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.Utils
import org.apache.spark.Partition
import org.apache.spark.SparkContext
import org.apache.spark.SparkEnv
import org.apache.spark.SparkException
import org.apache.spark.TaskContext
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageInput
import org.apache.spark.sql.execution.adaptive.LocalShuffledRowRDD
import org.apache.spark.sql.execution.adaptive.QueryStage
import org.apache.spark.sql.execution.adaptive.QueryStageInput
import org.apache.spark.sql.execution.adaptive.ShuffleQueryStageInput
import org.apache.spark.sql.execution.adaptive.SkewedShuffleQueryStageInput
import org.apache.spark.ShuffleDependency
import org.apache.spark.sql.execution.blaze.plan.ArrowShuffleExchangeExec
import org.apache.spark.OneToOneDependency
import org.blaze.protobuf.IpcReaderExecNode
import org.blaze.protobuf.IpcReadMode
import org.blaze.protobuf.PartitionId
import org.blaze.protobuf.PhysicalPlanNode
import org.blaze.protobuf.TaskDefinition

trait NativeSupports extends SparkPlan {
  implicit class ImplicitLogicalLink(sparkPlan: SparkPlan)
      extends BlazeConverters.ImplicitLogicalLink(sparkPlan)

  def doExecuteNative(): NativeRDD

  protected override def doExecute(): RDD[InternalRow] = doExecuteNative()
  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = doExecuteNative().toColumnar

}

object NativeSupports extends Logging {

  val currentUser: UserGroupInformation = UserGroupInformation.getCurrentUser

  val blazeOperatorMetricsCollector: Option[BlazeOperatorMetricsCollector] =
    if (isBlazeOperatorMetricsEnabled) {
      Some(new BlazeOperatorMetricsCollector)
    } else {
      None
    }

  @tailrec
  def isNative(plan: SparkPlan): Boolean =
    plan match {
      case _: NativeSupports => true
      case plan: QueryStageInput => isNative(plan.childStage)
      case plan: QueryStage => isNative(plan.child)
      case plan: ReusedExchangeExec => isNative(plan.child)
      case _ => false
    }

  @tailrec
  def getUnderlyingNativePlan(plan: SparkPlan): NativeSupports =
    plan match {
      case plan: NativeSupports => plan
      case plan: QueryStageInput => getUnderlyingNativePlan(plan.childStage)
      case plan: QueryStage => getUnderlyingNativePlan(plan.child)
      case plan: ReusedExchangeExec => getUnderlyingNativePlan(plan.child)
      case _ => throw new RuntimeException("unreachable: plan is not native")
    }

  @tailrec
  def executeNative(plan: SparkPlan): NativeRDD =
    plan match {
      case plan: NativeSupports =>
        NativeSupports.blazeOperatorMetricsCollector.foreach(
          _.createListener(plan, plan.sparkContext))
        plan.doExecuteNative()
      case plan: ShuffleQueryStageInput => executeNativeCustomShuffleReader(plan, plan.output)
      case plan: SkewedShuffleQueryStageInput =>
        executeNativeCustomShuffleReader(plan, plan.output)
      case plan: BroadcastQueryStageInput => executeNative(plan.childStage)
      case plan: QueryStage => executeNative(plan.child)
      case plan: ReusedExchangeExec => executeNative(plan.child)
      case _ => throw new SparkException(s"Underlying plan is not NativeSupports: ${plan}")
    }

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
      "join_time" -> SQLMetrics.createNanoTimingMetric(sc, "Native.join_time"))

  private def executeNativeCustomShuffleReader(
      exec: SparkPlan,
      output: Seq[Attribute]): NativeRDD = {
    exec match {
      case _: ShuffleQueryStageInput | _: SkewedShuffleQueryStageInput =>
        val shuffledRDD = exec.execute()
        val dependency = shuffledRDD.getClass
          .getMethod("dependency")
          .invoke(shuffledRDD)
          .asInstanceOf[ShuffleDependency[_, _, _]]
        val shuffleHandle = dependency.shuffleHandle

        val shuffleExec = exec
          .asInstanceOf[QueryStageInput]
          .childStage
          .child
          .asInstanceOf[ArrowShuffleExchangeExec]
        val inputMetrics = shuffleExec.metrics
        val inputRDD = exec match {
          case exec: ShuffleQueryStageInput => NativeSupports.executeNative(exec.childStage)
          case exec: SkewedShuffleQueryStageInput => NativeSupports.executeNative(exec.childStage)
        }

        val nativeSchema = shuffleExec.nativeSchema
        val metrics = inputRDD.metrics
        val partitionClsName = shuffledRDD.getClass.getSimpleName

        new NativeRDD(
          shuffledRDD.sparkContext,
          metrics,
          shuffledRDD.partitions,
          new OneToOneDependency(shuffledRDD) :: Nil,
          shuffledRDD.shuffleReadFull,
          (partition, taskContext) => {
            val shuffleReadMetrics = taskContext.taskMetrics().createTempShuffleReadMetrics()
            val metricsReporter =
              new SQLShuffleReadMetricsReporter(shuffleReadMetrics, inputMetrics)

            val classOfShuffledRowRDDPartition =
              Utils.classForName("org.apache.spark.sql.execution.ShuffledRowRDDPartition")
            val classOfAdaptiveShuffledRowRDDPartition =
              Utils.classForName(
                "org.apache.spark.sql.execution.adaptive.AdaptiveShuffledRowRDDPartition")

            val readers: Iterator[ShuffleReader[_, _]] = shuffledRDD match {
              case rdd: LocalShuffledRowRDD =>
                val shuffledRowPartition = partition.asInstanceOf[ShuffledRDDPartition]
                val mapId = shuffledRowPartition.index
                val partitionStartIndices = rdd.partitionStartIndices.iterator
                val partitionEndIndices = rdd.partitionEndIndices.iterator
                partitionStartIndices
                  .zip(partitionEndIndices)
                  .map {
                    case (start, end) =>
                      logInfo(
                        s"Create local shuffle reader mapId $mapId, partition range $start-$end")
                      SparkEnv.get.shuffleManager
                        .getReader(
                          shuffleHandle,
                          start,
                          end,
                          taskContext,
                          metricsReporter,
                          mapId,
                          mapId + 1)
                  }
              case _ =>
                partition match {
                  case p if classOfShuffledRowRDDPartition.isInstance(p) =>
                    val clz = classOfShuffledRowRDDPartition
                    val startPreShufflePartitionIndex =
                      clz.getMethod("startPreShufflePartitionIndex").invoke(p).asInstanceOf[Int]
                    val endPreShufflePartitionIndex =
                      clz.getMethod("endPreShufflePartitionIndex").invoke(p).asInstanceOf[Int]

                    Iterator.single(
                      SparkEnv.get.shuffleManager
                        .getReader(
                          shuffleHandle,
                          startPreShufflePartitionIndex,
                          endPreShufflePartitionIndex,
                          taskContext,
                          metricsReporter))

                  case p if classOfAdaptiveShuffledRowRDDPartition.isInstance(p) =>
                    val clz = classOfAdaptiveShuffledRowRDDPartition
                    val preShufflePartitionIndex =
                      clz.getMethod("preShufflePartitionIndex").invoke(p).asInstanceOf[Int]
                    val startMapId = clz.getMethod("startMapId").invoke(p).asInstanceOf[Int]
                    val endMapId = clz.getMethod("endMapId").invoke(p).asInstanceOf[Int]

                    Iterator.single(
                      SparkEnv.get.shuffleManager
                        .getReader(
                          shuffleHandle,
                          preShufflePartitionIndex,
                          preShufflePartitionIndex + 1,
                          taskContext,
                          metricsReporter,
                          startMapId,
                          endMapId))
                  case p =>
                    Iterator.single(
                      SparkEnv.get.shuffleManager.getReader(
                        shuffleHandle,
                        p.index,
                        p.index + 1,
                        taskContext,
                        metricsReporter))
                }
            }

            // store fetch iterator in jni resource before native compute
            val jniResourceId = s"NativeShuffleReadExec:${UUID.randomUUID().toString}"
            JniBridge.resourcesMap.put(
              jniResourceId,
              () => {
                CompletionIterator[Object, Iterator[Object]](
                  readers.flatMap(_.asInstanceOf[ArrowBlockStoreShuffleReader[_, _]].readIpc()),
                  taskContext.taskMetrics().mergeShuffleReadMetrics(true))
              })

            PhysicalPlanNode
              .newBuilder()
              .setIpcReader(
                IpcReaderExecNode
                  .newBuilder()
                  .setSchema(nativeSchema)
                  .setNumPartitions(shuffledRDD.getNumPartitions)
                  .setIpcProviderResourceId(jniResourceId)
                  .setMode(IpcReadMode.CHANNEL_AND_FILE_SEGMENT)
                  .build())
              .build()
          },
          friendlyName = s"NativeRDD.ShuffleRead [$partitionClsName]")
    }
  }
}

case class MetricNode(
    metrics: Map[String, SQLMetric],
    children: Seq[MetricNode],
    metricValueHandler: Option[(String, Long) => Unit] = None)
    extends Logging {

  def getChild(i: Int): MetricNode =
    children(i)

  def add(metricName: String, v: Long): Unit = {
    metrics.get(metricName) match {
      case Some(metric) => metric.add(v)
      case None =>
        metricValueHandler match {
          case Some(handler) => handler.apply(metricName, v)
          case None =>
            logWarning(s"Ignore non-exist metric: ${metricName}")
        }
    }
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

  BlazeCallNativeWrapper.synchronized {
    val conf = SparkEnv.get.conf
    val batchSize = conf.getLong("spark.blaze.batchSize", 16384);
    val nativeMemory = conf.getLong("spark.executor.memoryOverhead", Long.MaxValue) * 1024 * 1024;
    val memoryFraction = conf.getDouble("spark.blaze.memoryFraction", 0.75);
    val tmpDirs =
      SparkEnv.get.blockManager.diskBlockManager.localDirs.map(_.toString).mkString(",")

    if (!BlazeCallNativeWrapper.nativeInitialized) {
      logInfo(s"Initializing native environment ...")
      BlazeCallNativeWrapper.load("blaze")
      JniBridge.initNative(batchSize, nativeMemory, memoryFraction, tmpDirs)
      BlazeCallNativeWrapper.nativeInitialized = true
    }
  }

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

object BlazeCallNativeWrapper {
  private var nativeInitialized: Boolean = false

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
