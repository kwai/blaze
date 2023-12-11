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
import java.io.IOException
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.util
import java.util.concurrent.atomic.AtomicReference

import org.apache.arrow.c.ArrowArray
import org.apache.arrow.c.ArrowSchema
import org.apache.arrow.c.CDataDictionaryProvider
import org.apache.arrow.c.Data
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.execution.blaze.arrowio.util.ArrowUtils
import org.apache.spark.sql.execution.blaze.arrowio.ColumnarHelper
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.Utils
import org.blaze.protobuf.PartitionId
import org.blaze.protobuf.PhysicalPlanNode
import org.blaze.protobuf.TaskDefinition

case class BlazeCallNativeWrapper(
    nativePlan: PhysicalPlanNode,
    partition: Partition,
    context: Option[TaskContext],
    metrics: MetricNode)
    extends Logging {

  BlazeCallNativeWrapper.initNative()

  private val error: AtomicReference[Throwable] = new AtomicReference(null)
  private val dictionaryProvider = new CDataDictionaryProvider()
  private val recordsQueue = new util.ArrayDeque[InternalRow]()
  private var arrowSchema: Schema = _

  logInfo(s"Start executing native plan")
  private var nativeRuntimePtr = JniBridge.callNative(NativeHelper.nativeMemory, this)

  private lazy val rowIterator = new Iterator[InternalRow] {
    override def hasNext: Boolean =
      !recordsQueue.isEmpty || JniBridge.nextBatch(nativeRuntimePtr)

    override def next(): InternalRow =
      recordsQueue.poll()
  }

  context.foreach(_.addTaskCompletionListener[Unit]((_: TaskContext) => close()))
  context.foreach(_.addTaskFailureListener((_, _) => close()))

  def getRowIterator: Iterator[InternalRow] = {
    CompletionIterator[InternalRow, Iterator[InternalRow]](rowIterator, this.close())
  }

  protected def getMetrics: MetricNode =
    metrics

  protected def importSchema(ffiSchemaPtr: Long): Unit = {
    val ffiSchema = ArrowSchema.wrap(ffiSchemaPtr)
    arrowSchema = Data.importSchema(ArrowUtils.rootAllocator, ffiSchema, dictionaryProvider)
  }

  protected def importBatch(ffiArrayPtr: Long): Unit = {
    checkError()

    val root: VectorSchemaRoot = VectorSchemaRoot.create(arrowSchema, ArrowUtils.rootAllocator)
    val ffiArray = ArrowArray.wrap(ffiArrayPtr)
    Utils.tryWithSafeFinally {
      Data.importIntoVectorSchemaRoot(
        ArrowUtils.rootAllocator,
        ffiArray,
        root,
        dictionaryProvider)

      val toUnsafe = UnsafeProjection.create(ArrowUtils.fromArrowSchema(root.getSchema))
      toUnsafe.initialize(Option(TaskContext.get()).map(_.partitionId()).getOrElse(0))

      val batch = ColumnarHelper.rootAsBatch(root)
      for (row <- ColumnarHelper.batchAsRowIter(batch)) {
        checkError()
        recordsQueue.offer(toUnsafe(row).copy())
      }
    } {
      root.close()
      ffiArray.close()
    }
  }

  protected def setError(error: Throwable): Unit = {
    this.error.set(error)
  }

  protected def checkError(): Unit = {
    val throwable = error.getAndSet(null)
    if (throwable != null) {
      throw throwable
    }
  }

  protected def getRawTaskDefinition: Array[Byte] = {
    val partitionId: PartitionId = PartitionId
      .newBuilder()
      .setPartitionId(partition.index)
      .setStageId(context.map(_.stageId()).getOrElse(0))
      .setJobId(partition.index.toString)
      .build()

    val taskDefinition = TaskDefinition
      .newBuilder()
      .setTaskId(partitionId)
      .setPlan(nativePlan)
      .build()
    taskDefinition.toByteArray
  }

  private def close(): Unit = {
    synchronized {
      if (nativeRuntimePtr != 0) {
        JniBridge.finalizeNative(nativeRuntimePtr)
        nativeRuntimePtr = 0
      }
      checkError()
    }
  }
}

object BlazeCallNativeWrapper extends Logging {
  def initNative(): Unit = {
    lazyInitNative
  }

  private lazy val lazyInitNative: Unit = {
    logInfo(
      "Initializing native environment (" +
        s"batchSize=${BlazeConf.BATCH_SIZE.intConf()}, " +
        s"nativeMemory=${NativeHelper.nativeMemory}, " +
        s"memoryFraction=${BlazeConf.MEMORY_FRACTION.doubleConf()}")
    BlazeCallNativeWrapper.loadLibBlaze()
  }

  private def loadLibBlaze(): Unit = {
    val libName = System.mapLibraryName("blaze")
    try {
      val classLoader = classOf[NativeSupports].getClassLoader
      val tempFile = File.createTempFile("libblaze-", ".tmp")
      tempFile.deleteOnExit()

      Utils.tryWithResource(classLoader.getResourceAsStream(libName)) { is =>
        assert(is != null, s"cannot load $libName")
        Files.copy(is, tempFile.toPath, StandardCopyOption.REPLACE_EXISTING)
      }
      System.load(tempFile.getAbsolutePath)

    } catch {
      case e: IOException =>
        throw new IllegalStateException("error loading native libraries: " + e)
    }
  }
}
