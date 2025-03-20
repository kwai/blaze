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

import java.io.{FileInputStream, InputStream}
import java.nio.ByteBuffer
import java.nio.channels.{Channels, ReadableByteChannel}
import scala.annotation.tailrec

import org.apache.commons.lang3.reflect.{FieldUtils, MethodUtils}
import org.apache.spark.{InterruptibleIterator, ShuffleDependency, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.LimitedInputStream
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleReader}
import org.apache.spark.storage.BlockId

abstract class BlazeBlockStoreShuffleReaderBase[K, C](
    handle: BaseShuffleHandle[K, _, C],
    context: TaskContext)
    extends ShuffleReader[K, C]
    with Logging {
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

object BlazeBlockStoreShuffleReaderBase extends Logging {
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

  @tailrec
  private def unwrapInputStream(in: InputStream): InputStream = {
    val bufferReleasingInputStreamClsName = "org.apache.spark.storage.BufferReleasingInputStream"
    in match {
      case in if bufferReleasingInputStreamClsName.endsWith(in.getClass.getName) =>
        val inner = MethodUtils.invokeMethod(in, true, "delegate").asInstanceOf[InputStream]
        unwrapInputStream(inner)
      case in => in
    }
  }

  def getFileSegmentFromInputStream(in: InputStream): Option[(String, Long, Long)] = {
    unwrapInputStream(in) match {
      case in: LimitedInputStream =>
        val left = FieldUtils.readDeclaredField(in, "left", true).asInstanceOf[Long]
        val inner = FieldUtils.readField(in, "in", true).asInstanceOf[InputStream]
        inner match {
          case in: FileInputStream =>
            val path = FieldUtils.readDeclaredField(in, "path", true).asInstanceOf[String]
            val offset = in.getChannel.position()
            Some((path, offset, left))
          case _ =>
            None
        }
      case _ =>
        None
    }
  }

  def getByteBufferFromInputStream(in: InputStream): Option[ByteBuffer] = {
    val byteBufferClsName = "io.netty.buffer.ByteBufInputStream"
    unwrapInputStream(in) match {
      case in if byteBufferClsName.endsWith(in.getClass.getName) =>
        val buffer = FieldUtils.readDeclaredField(in, "buffer", true)
        val nioBuffer = MethodUtils.invokeMethod(buffer, "nioBuffer").asInstanceOf[ByteBuffer]
        Some(nioBuffer)

      case in: InputStreamToByteBuffer =>
        Some(in.toByteBuffer)
      case in =>
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
