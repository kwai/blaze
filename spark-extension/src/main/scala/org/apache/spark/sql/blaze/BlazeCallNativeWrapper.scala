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
import java.util.concurrent.atomic.AtomicReference

import com.google.protobuf.CodedOutputStream
import org.apache.spark.InterruptibleIterator
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.blaze.protobuf.PartitionId
import org.blaze.protobuf.PhysicalPlanNode
import org.blaze.protobuf.TaskDefinition

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.blaze.arrowio.ArrowFFIStreamImportIterator
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.CompletionIterator

case class BlazeCallNativeWrapper(
    nativePlan: PhysicalPlanNode,
    partition: Partition,
    context: Option[TaskContext],
    metrics: MetricNode)
    extends Logging {

  BlazeCallNativeWrapper.initNative()

  private val error: AtomicReference[Throwable] = new AtomicReference(null)
  private var arrowFFIStreamPtr = 0L

  logInfo(s"Start executing native plan")
  private var nativeRuntimePtr = JniBridge.callNative(this)
  private var rowIterator = {
    val iter = new ArrowFFIStreamImportIterator(context, arrowFFIStreamPtr, _ => checkError())
    context match {
      case Some(tc) => new InterruptibleIterator[InternalRow](tc, iter)
      case None => iter
    }
  }

  context.foreach(_.addTaskCompletionListener(_ => close()))
  context.foreach(_.addTaskFailureListener((_, _) => close()))

  def getRowIterator: Iterator[InternalRow] = {
    CompletionIterator[InternalRow, Iterator[InternalRow]](rowIterator, this.close())
  }

  protected def getMetrics: MetricNode =
    metrics

  protected def setError(error: Throwable): Unit = {
    this.error.set(error)
  }

  protected def checkError(): Unit = {
    val throwable = error.getAndSet(null)
    if (throwable != null) {
      throw throwable
    }
  }

  protected def setArrowFFIStreamPtr(ptr: Long): Unit = {
    this.arrowFFIStreamPtr = ptr
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
      if (rowIterator != null) {
        rowIterator match {
          case iter: InterruptibleIterator[_] =>
            iter.delegate.asInstanceOf[ArrowFFIStreamImportIterator].close()
          case iter: ArrowFFIStreamImportIterator =>
            iter.close()
        }
        rowIterator = null
      }
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
