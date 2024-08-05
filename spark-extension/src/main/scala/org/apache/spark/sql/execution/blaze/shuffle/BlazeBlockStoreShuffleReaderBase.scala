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

import java.io.FileInputStream
import java.io.FilterInputStream
import java.io.InputStream
import java.lang.reflect.Field
import java.lang.reflect.Method
import java.nio.channels.Channels
import java.nio.channels.ReadableByteChannel
import java.nio.ByteBuffer

import scala.annotation.tailrec

import org.apache.spark.InterruptibleIterator
import org.apache.spark.ShuffleDependency
import org.apache.spark.TaskContext
import org.apache.spark.network.util.LimitedInputStream
import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.spark.shuffle.ShuffleReader
import org.apache.spark.storage.BlockId

import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufInputStream

abstract class BlazeBlockStoreShuffleReaderBase[K, C](
    handle: BaseShuffleHandle[K, _, C],
    context: TaskContext)
    extends ShuffleReader[K, C] {
  import BlazeBlockStoreShuffleReaderBase._

  protected val dep: ShuffleDependency[K, _, C] = handle.dependency
  protected def readBlocks(): Iterator[(BlockId, InputStream)]

  def readIpc(): Iterator[BlockObject] = {
    val ipcIterator = readBlocks().map { case (_, inputStream) =>
      createBlockObject(inputStream)
    }

    // An interruptible iterator must be used here in order to support task cancellation
    new InterruptibleIterator[BlockObject](context, ipcIterator)
  }

  /** Read the combined key-values for this reduce task */
  override def read(): Iterator[Product2[K, C]] =
    throw new NotImplementedError(
      "arrow shuffle reader does not support non-native read() method")
}

object BlazeBlockStoreShuffleReaderBase {
  def createBlockObject(in: InputStream): BlockObject = {
    getFileSegmentFromInputStream(in) match {
      case Some((path, offset, limit)) =>
        return new BlockObject {
          override def hasFileSegment: Boolean = true
          override def getFilePath: String = path
          override def getFileOffset: Long = offset
          override def getFileLength: Long = limit
          override def close(): Unit = in.close()
        }
      case None =>
    }

    getByteBufferFromInputStream(in) match {
      case Some(buf) =>
        return new BlockObject {
          override def hasByteBuffer: Boolean = true
          override def getByteBuffer: ByteBuffer = buf
          override def close(): Unit = in.close()
        }
      case None =>
    }

    val channel = Channels.newChannel(in)
    new BlockObject {
      override def getChannel: ReadableByteChannel = channel
      override def close(): Unit = channel.close()
    }
  }

  private lazy val bufferReleasingInputStreamClass: Class[_] =
    Class.forName("org.apache.spark.storage.BufferReleasingInputStream")
  private lazy val delegateFn: Method =
    bufferReleasingInputStreamClass.getDeclaredMethods.find(_.getName.endsWith("delegate")).get
  private lazy val inField: Field = classOf[FilterInputStream].getDeclaredField("in")
  private lazy val limitField: Field = classOf[LimitedInputStream].getDeclaredField("left")
  private lazy val pathField: Field = classOf[FileInputStream].getDeclaredField("path")
  delegateFn.setAccessible(true)
  inField.setAccessible(true)
  limitField.setAccessible(true)
  pathField.setAccessible(true)

  private lazy val timeTrackingInputStreamClass: Class[_] =
    Class.forName("org.apache.spark.storage.TimeTrackingInputStream")
  private lazy val inputStreamField = {
    val f = timeTrackingInputStreamClass.getDeclaredField("inputStream")
    f.setAccessible(true)
    f
  }

  private lazy val byteBufInputStreamClass: Class[_] =
    Class.forName("io.netty.buffer.ByteBufInputStream")
  private lazy val bufferField = byteBufInputStreamClass.getDeclaredField("buffer")
  private lazy val startIndexField = byteBufInputStreamClass.getDeclaredField("startIndex")
  private lazy val endIndexField = byteBufInputStreamClass.getDeclaredField("endIndex")
  bufferField.setAccessible(true)
  startIndexField.setAccessible(true)
  endIndexField.setAccessible(true)

  @tailrec
  private def unwrapInputStream(in: InputStream): InputStream = {
    in match {
      case in if bufferReleasingInputStreamClass.isInstance(in) =>
        unwrapInputStream(delegateFn.invoke(in).asInstanceOf[InputStream])
      // case in: TimeTrackingInputStream =>
      //  unwrapInputStream(inputStreamField.get(in).asInstanceOf[InputStream])
      case in => in
    }
  }

  def getFileSegmentFromInputStream(in: InputStream): Option[(String, Long, Long)] = {
    unwrapInputStream(in) match {
      case in: LimitedInputStream =>
        val limit = limitField.getLong(in)
        inField.get(in) match {
          case in: FileInputStream =>
            val path = pathField.get(in).asInstanceOf[String]
            val offset = in.getChannel.position()
            Some((path, offset, limit))
          case _ =>
            None
        }
      case _ =>
        None
    }
  }

  def getByteBufferFromInputStream(in: InputStream): Option[ByteBuffer] = {
    unwrapInputStream(in) match {
      case in: ByteBufInputStream =>
        Some(bufferField.get(in).asInstanceOf[ByteBuf].internalNioBuffer(0, in.available()))
      case in: InputStreamToByteBuffer =>
        Some(in.toByteBuffer)
      case _ =>
        None
    }
  }
}

trait InputStreamToByteBuffer {
  def toByteBuffer: ByteBuffer
}

trait BlockObject extends AutoCloseable {
  def hasFileSegment: Boolean = false
  def hasByteBuffer: Boolean = false
  def getFilePath: String = throw new UnsupportedOperationException
  def getFileOffset: Long = throw new UnsupportedOperationException
  def getFileLength: Long = throw new UnsupportedOperationException
  def getByteBuffer: ByteBuffer = throw new UnsupportedOperationException
  def getChannel: ReadableByteChannel = throw new UnsupportedOperationException
}
