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

import java.io.File
import java.io.FileInputStream
import java.io.FilterInputStream
import java.io.InputStream
import java.lang.reflect.Field
import java.lang.reflect.Method

import org.apache.spark.InterruptibleIterator
import org.apache.spark.ShuffleDependency
import org.apache.spark.TaskContext

import org.apache.spark.network.util.LimitedInputStream
import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.spark.shuffle.ShuffleReader
import org.apache.spark.shuffle.ShuffleReadMetricsReporter
import org.apache.spark.sql.blaze.Shims
import org.apache.spark.sql.execution.blaze.arrowio.ArrowReaderIterator
import org.apache.spark.sql.execution.blaze.arrowio.IpcInputStreamIterator
import org.apache.spark.sql.execution.blaze.shuffle.ArrowBlockStoreShuffleReaderBase.getFileSegmentFromInputStream
import org.apache.spark.storage.BlockId
import org.apache.spark.storage.FileSegment
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.Utils

abstract class ArrowBlockStoreShuffleReaderBase[K, C](
    handle: BaseShuffleHandle[K, _, C],
    context: TaskContext,
    readMetrics: ShuffleReadMetricsReporter)
    extends ShuffleReader[K, C] {

  protected val dep: ShuffleDependency[K, _, C] = handle.dependency
  protected def readBlocks(): Iterator[(BlockId, InputStream)]

  def readIpc(): Iterator[Object] = { // FileSegment | ReadableByteChannel
    val ipcIterator = readBlocks().flatMap {
      case (_, inputStream) =>
        getFileSegmentFromInputStream(inputStream) match {
          case Some(fileSegment) => Iterator.single(fileSegment)
          case None =>
            IpcInputStreamIterator(inputStream, decompressingNeeded = false, context)
        }
    }

    // An interruptible iterator must be used here in order to support task cancellation
    new InterruptibleIterator[Object](context, ipcIterator)
  }

  /** Read the combined key-values for this reduce task */
  override def read(): Iterator[Product2[K, C]] = {
    val recordIter = readBlocks()
      .flatMap {
        case (_, inputStream) =>
          IpcInputStreamIterator(inputStream, decompressingNeeded = true, context)
      }
      .flatMap { channel =>
        new ArrowReaderIterator(channel, context).map((0, _)) // use 0 as key since it's not used
      }

    // Update the context task metrics for each record read.
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      recordIter.map { record =>
        readMetrics.incRecordsRead(1)
        record
      },
      context.taskMetrics().mergeShuffleReadMetrics())

    // An interruptible iterator must be used here in order to support task cancellation
    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)

    val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
      throw new UnsupportedOperationException("aggregate not allowed")
    } else {
      interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
    }

    // Sort the output if there is a sort ordering defined.
    val resultIter = dep.keyOrdering match {
      case Some(keyOrd: Ordering[K]) =>
        throw new UnsupportedOperationException("order not allowed")
      case None =>
        aggregatedIter
    }

    resultIter match {
      case _: InterruptibleIterator[Product2[K, C]] => resultIter
      case _ =>
        // Use another interruptible iterator here to support task cancellation as aggregator
        // or(and) sorter may have consumed previous interruptible iterator.
        new InterruptibleIterator[Product2[K, C]](context, resultIter)
    }
  }

}

object ArrowBlockStoreShuffleReaderBase {
  private val bufferReleasingInputStreamClass: Class[_] =
    Class.forName("org.apache.spark.storage.BufferReleasingInputStream")
  private val delegateFn: Method =
    bufferReleasingInputStreamClass.getDeclaredMethods.find(_.getName.endsWith("delegate")).get
  private val inField: Field = classOf[FilterInputStream].getDeclaredField("in")
  private val limitField: Field = classOf[LimitedInputStream].getDeclaredField("left")
  private val pathField: Field = classOf[FileInputStream].getDeclaredField("path")
  delegateFn.setAccessible(true)
  inField.setAccessible(true)
  limitField.setAccessible(true)
  pathField.setAccessible(true)

  def getFileSegmentFromInputStream(in: InputStream): Option[FileSegment] = {

    delegateFn.invoke(in) match {
      case in: LimitedInputStream =>
        val limit = limitField.getLong(in)
        inField.get(in) match {
          case in: FileInputStream =>
            val path = pathField.get(in).asInstanceOf[String]
            val offset = in.getChannel.position()
            val fileSegment =
              Shims.get.shuffleShims.createFileSegment(new File(path), offset, limit, 0)
            Some(fileSegment)
          case _ =>
            None
        }
      case _ =>
        None
    }
  }
}
