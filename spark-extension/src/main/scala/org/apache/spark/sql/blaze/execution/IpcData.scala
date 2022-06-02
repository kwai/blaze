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

package org.apache.spark.sql.blaze.execution

import java.io.EOFException
import java.nio.channels.Channels
import java.nio.channels.ReadableByteChannel
import java.nio.ByteBuffer

import org.apache.commons.io.input.CountingInputStream
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.LimitedInputStream

case class IpcData(
    channel: ReadableByteChannel,
    compressed: Boolean,
    ipcLength: Long,
    ipcLengthUncompressed: Long)
    extends Logging {

  def readArrowData(buf: ByteBuffer): Unit = {
    assert(
      buf.remaining() == ipcLengthUncompressed,
      s"Data corrupt: buf.remaining != ipcLengthUncompressed (${buf.remaining()} -> $ipcLengthUncompressed)")

    if (!compressed) {
      while (buf.hasRemaining && channel.read(buf) >= 0) {}
      if (buf.hasRemaining) {
        throw new EOFException(
          "Data corrupt: unexpected EOF while reading uncompressed ipc content")
      }
      return
    }

    val is = new LimitedInputStream(Channels.newInputStream(channel), ipcLength, false)
    val isCounting = new CountingInputStream(is)
    val zs = ArrowShuffleManager301.compressionCodecForShuffling.compressedInputStream(isCounting)
    val zsCounting = new CountingInputStream(zs)

    val zchannel = Channels.newChannel(zsCounting)
    while (buf.hasRemaining && zchannel.read(buf) >= 0) {}

    assert(
      isCounting.getByteCount == ipcLength,
      s"Data corrupt: ipc_length not matched (expected: $ipcLength, actual: ${isCounting.getByteCount})")
    assert(
      zsCounting.getByteCount == ipcLengthUncompressed,
      s"Data corrupt: ipc_length_uncompressed not matched (expected: $ipcLengthUncompressed, actual: ${zsCounting.getByteCount})")
  }
}
