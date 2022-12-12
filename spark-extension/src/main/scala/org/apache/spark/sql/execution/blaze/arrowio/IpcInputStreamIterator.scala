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

package org.apache.spark.sql.execution.blaze.arrowio

import java.io.EOFException
import java.io.InputStream
import java.nio.channels.Channels
import java.nio.channels.ReadableByteChannel
import java.nio.ByteBuffer
import java.nio.ByteOrder

import org.apache.spark.internal.Logging
import org.apache.spark.network.util.LimitedInputStream
import org.apache.spark.TaskContext

import org.apache.spark.sql.execution.blaze.shuffle.ArrowShuffleDependency

case class IpcInputStreamIterator(
    var in: InputStream,
    decompressingNeeded: Boolean,
    taskContext: TaskContext)
    extends Iterator[ReadableByteChannel]
    with Logging {

  private[execution] val channel: ReadableByteChannel = Channels.newChannel(in)
  private val ipcLengthsBuf = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)

  // NOTE:
  // since all ipcs are sharing the same input stream and channel, the second
  // hasNext() must be called after the first ipc has been completely processed.

  private[execution] var consumed = true
  private var finished = false
  private var currentIpcLength = 0L
  private var currentLimitedInputStream: LimitedInputStream = _

  if (taskContext != null) {
    taskContext.addTaskCompletionListener[Unit](_ => {
      closeInputStream()
    })
  }

  override def hasNext: Boolean = {
    if (finished) {
      return false
    }
    if (!consumed) {
      return true
    }
    if (currentLimitedInputStream != null) {
      currentLimitedInputStream.skip(Int.MaxValue)
      currentLimitedInputStream = null
    }

    ipcLengthsBuf.clear()
    while (ipcLengthsBuf.hasRemaining && channel.read(ipcLengthsBuf) >= 0) {}

    if (ipcLengthsBuf.hasRemaining) {
      if (ipcLengthsBuf.position() == 0) {
        finished = true
        closeInputStream()
        return false
      }
      throw new EOFException("Data corrupt: unexpected EOF while reading compressed ipc lengths")
    }
    ipcLengthsBuf.flip()
    currentIpcLength = ipcLengthsBuf.getLong

    if (currentIpcLength == 0) { // skip empty ipc
      return hasNext
    }
    consumed = false
    true
  }

  override def next(): ReadableByteChannel = {
    assert(!consumed)
    consumed = true

    val is = new LimitedInputStream(Channels.newInputStream(channel), currentIpcLength, false)
    currentLimitedInputStream = is

    if (decompressingNeeded) {
      val zs = ZstdUtil.createZstdInputStreamWithIpcDict(is)
      Channels.newChannel(zs)
    } else {
      Channels.newChannel(is)
    }
  }

  def closeInputStream(): Unit =
    synchronized {
      if (in != null) {
        in.close()
        in = null
      }
    }
}
