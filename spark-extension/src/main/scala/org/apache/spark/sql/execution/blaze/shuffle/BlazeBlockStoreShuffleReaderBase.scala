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
import java.nio.channels.Channels

import org.apache.spark.InterruptibleIterator
import org.apache.spark.ShuffleDependency
import org.apache.spark.TaskContext

import org.apache.spark.network.util.LimitedInputStream
import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.spark.shuffle.ShuffleReader
import org.apache.spark.sql.blaze.Shims
import org.apache.spark.storage.BlockId
import org.apache.spark.storage.FileSegment

abstract class BlazeBlockStoreShuffleReaderBase[K, C](
    handle: BaseShuffleHandle[K, _, C],
    context: TaskContext)
    extends ShuffleReader[K, C] {
  import BlazeBlockStoreShuffleReaderBase._

  protected val dep: ShuffleDependency[K, _, C] = handle.dependency
  protected def readBlocks(): Iterator[(BlockId, InputStream)]

  def readIpc(): Iterator[Object] = { // FileSegment | ReadableByteChannel
    val ipcIterator = readBlocks().map { case (_, inputStream) =>
      getFileSegmentFromInputStream(inputStream) match {
        case Some(fileSegment) =>
          fileSegment
        case None =>
          Channels.newChannel(inputStream)
      }
    }

    // An interruptible iterator must be used here in order to support task cancellation
    new InterruptibleIterator[Object](context, ipcIterator)
  }

  /** Read the combined key-values for this reduce task */
  override def read(): Iterator[Product2[K, C]] =
    throw new NotImplementedError(
      "arrow shuffle reader does not support non-native read() method")
}

object BlazeBlockStoreShuffleReaderBase {
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
    if (!bufferReleasingInputStreamClass.isInstance(in)) {
      return None
    }
    delegateFn.invoke(in) match {
      case in: LimitedInputStream =>
        val limit = limitField.getLong(in)
        inField.get(in) match {
          case in: FileInputStream =>
            val path = pathField.get(in).asInstanceOf[String]
            val offset = in.getChannel.position()
            val fileSegment =
              Shims.get.createFileSegment(new File(path), offset, limit, 0)
            Some(fileSegment)
          case _ =>
            None
        }
      case _ =>
        None
    }
  }
}
