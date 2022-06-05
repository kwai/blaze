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
import java.util.concurrent.SynchronousQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.UUID

import scala.annotation.tailrec
import scala.collection.immutable.TreeMap

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.adaptive.CustomShuffleReaderExec
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.SparkEnv
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.Partition
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.blaze.execution.ArrowBlockStoreShuffleReader301
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.CoalescedPartitionSpec
import org.apache.spark.sql.execution.ShuffledRowRDD
import org.apache.spark.sql.execution.ShufflePartitionSpec
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.blaze.protobuf.PartitionId
import org.blaze.protobuf.PhysicalPlanNode
import org.blaze.protobuf.Schema
import org.blaze.protobuf.ShuffleReaderExecNode
import org.blaze.protobuf.TaskDefinition

trait NativeSupports extends SparkPlan {
  def doExecuteNative(): NativeRDD

  protected override def doExecute(): RDD[InternalRow] = doExecuteNative()
  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = doExecuteNative().toColumnar
}

object NativeSupports extends Logging {
  @tailrec
  def isNative(plan: SparkPlan): Boolean =
    plan match {
      case _: NativeSupports => true
      case plan: CustomShuffleReaderExec => isNative(plan.child)
      case plan: QueryStageExec => isNative(plan.plan)
      case plan: ReusedExchangeExec => isNative(plan.child)
      case _ => false
    }

  @tailrec
  def getUnderlyingNativePlan(plan: SparkPlan): NativeSupports =
    plan match {
      case plan: NativeSupports => plan
      case plan: CustomShuffleReaderExec => getUnderlyingNativePlan(plan.child)
      case plan: QueryStageExec => getUnderlyingNativePlan(plan.plan)
      case plan: ReusedExchangeExec => getUnderlyingNativePlan(plan.child)
      case _ => throw new RuntimeException("unreachable: plan is not native")
    }

  @tailrec
  def executeNative(plan: SparkPlan): NativeRDD =
    plan match {
      case plan: NativeSupports => plan.doExecuteNative()
      case plan: CustomShuffleReaderExec => executeNativeCustomShuffleReader(plan, plan.output)
      case plan: QueryStageExec => executeNative(plan.plan)
      case plan: ReusedExchangeExec => executeNative(plan.child)
      case _ => throw new SparkException(s"Underlying plan is not NativeSupports: ${plan}")
    }

  def executeNativePlan(
      nativePlan: PhysicalPlanNode,
      metrics: MetricNode,
      partition: Partition,
      context: TaskContext): Iterator[InternalRow] = {

    val wrapper = BlazeCallNativeWrapper(nativePlan, partition, context, metrics)
    FFIHelper.fromBlazeCallNative(wrapper, context)
  }

  def executeNativePlanColumnar(
      nativePlan: PhysicalPlanNode,
      metrics: MetricNode,
      partition: Partition,
      context: TaskContext): Iterator[ColumnarBatch] = {

    val wrapper = BlazeCallNativeWrapper(nativePlan, partition, context, metrics)
    FFIHelper.fromBlazeCallNativeColumnar(wrapper, context)
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
      exec: CustomShuffleReaderExec,
      output: Seq[Attribute]): NativeRDD = {
    exec match {
      case CustomShuffleReaderExec(_, _, _) =>
        val inputShuffledRowRDD = exec.execute().asInstanceOf[ShuffledRowRDD]
        val shuffleHandle = inputShuffledRowRDD.dependency.shuffleHandle

        val inputRDD = NativeSupports.executeNative(exec.child)
        val nativeSchema: Schema = NativeConverters.convertSchema(StructType(output.map(a =>
          StructField(a.toString(), a.dataType, a.nullable, a.metadata))))
        val metrics = MetricNode(Map(), Seq(inputRDD.metrics))

        new NativeRDD(
          inputShuffledRowRDD.sparkContext,
          metrics,
          inputShuffledRowRDD.partitions,
          inputShuffledRowRDD.dependencies,
          (partition, taskContext) => {

            // use reflection to get partitionSpec because ShuffledRowRDDPartition is private
            val shuffledRDDPartitionClass =
              Class.forName("org.apache.spark.sql.execution.ShuffledRowRDDPartition")
            val specField = shuffledRDDPartitionClass.getDeclaredField("spec")
            specField.setAccessible(true)
            val spec = specField.get(partition).asInstanceOf[ShufflePartitionSpec]

            spec match {
              case CoalescedPartitionSpec(startReducerIndex, endReducerIndex) =>
                // store fetch iterator in jni resource before native compute
                val jniResourceId = s"NativeShuffleReadExec:${UUID.randomUUID().toString}"
                JniBridge.resourcesMap.put(
                  jniResourceId,
                  () => {
                    val shuffleManager = SparkEnv.get.shuffleManager
                    shuffleManager
                      .getReader(
                        shuffleHandle,
                        startReducerIndex,
                        endReducerIndex,
                        taskContext,
                        taskContext.taskMetrics().createTempShuffleReadMetrics())
                      .asInstanceOf[ArrowBlockStoreShuffleReader301[_, _]]
                      .readIpc()
                  })

                PhysicalPlanNode
                  .newBuilder()
                  .setShuffleReader(
                    ShuffleReaderExecNode
                      .newBuilder()
                      .setSchema(nativeSchema)
                      .setNumPartitions(inputShuffledRowRDD.getNumPartitions)
                      .setNativeShuffleId(jniResourceId)
                      .build())
                  .build()

              case unsupported =>
                throw new NotImplementedError(
                  s"CustomShuffleReader partition spec is not yet supported: ${unsupported}")
            }
          })
    }
  }
}

case class MetricNode(metrics: Map[String, SQLMetric], children: Seq[MetricNode])
    extends Logging {
  def getChild(i: Int): MetricNode =
    children(i)

  def add(metricName: String, v: Long): Unit = {
    metrics.get(metricName) match {
      case Some(metric) => metric.add(v)
      case None =>
        logWarning(s"Ignore non-exist metric: ${metricName}")
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

  BlazeCallNativeWrapper.synchronized {
    val conf = SparkEnv.get.conf
    val batchSize = conf.getLong("spark.blaze.batchSize", 16384);
    val nativeMemory = conf.getLong("spark.executor.memoryOverhead", Long.MaxValue) * 1024 * 1024;
    val memoryFraction = conf.getDouble("spark.blaze.memoryFraction", 0.75);
    val tmpDirs = SparkEnv.get.blockManager.diskBlockManager.localDirsString.mkString(",")

    if (!BlazeCallNativeWrapper.nativeInitialized) {
      logInfo(s"Initializing native environment ...")
      BlazeCallNativeWrapper.load("blaze")
      JniBridge.initNative(batchSize, nativeMemory, memoryFraction, tmpDirs)
      BlazeCallNativeWrapper.nativeInitialized = true
    }
  }

  logInfo(s"Start executing native plan")
  JniBridge.callNative(this)

  def isFinished: Boolean = finished.get()
  def finish(): Unit = {
    if (!isFinished) {
      finished.set(true)
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
        throw e
      case null =>
      // do nothing
    }
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
