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

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.ByteOrder

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.FileSegmentManagedBuffer
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.buffer.NettyManagedBuffer
import org.apache.spark.network.buffer.NioManagedBuffer
import org.apache.spark.sql.catalyst.InternalRow
import org.blaze.FileSegmentSeekableByteChannel
import org.blaze.NioSeekableByteChannel

object Converters extends Logging {

  def readManagedBufferToSegmentByteChannels(data: ManagedBuffer): Seq[SeekableByteChannel] = {
    val result: ArrayBuffer[SeekableByteChannel] = ArrayBuffer()
    data match {
      case f: FileSegmentManagedBuffer =>
        val file = f.getFile
        val lengthReader = new RandomAccessFile(file, "r")
        var curEnd = f.getOffset + f.getLength

        while (curEnd > f.getOffset) {
          val lenBuf = ByteBuffer.allocate(8)
          lengthReader.seek(curEnd - 8)
          lengthReader.read(lenBuf.array())
          val len = lenBuf.order(ByteOrder.LITTLE_ENDIAN).getLong(0).toInt
          val curStart = curEnd - 8 - len
          val fsc = new FileSegmentSeekableByteChannel(file, curStart, len)
          result += fsc
          curEnd = curStart
        }

      case _: NettyManagedBuffer | _: NioManagedBuffer =>
        val all = data.nioByteBuffer()
        var curEnd = all.limit()

        while (curEnd > 0) {
          val lenBuf = ByteBuffer.allocate(8)
          all.position(curEnd - 8)
          lenBuf.putLong(all.getLong)
          val len = lenBuf.order(ByteOrder.LITTLE_ENDIAN).getLong(0).toInt
          val curStart = curEnd - 8 - len
          val sc = new NioSeekableByteChannel(all, curStart, len)
          result += sc
          curEnd = curStart
        }
      case mb =>
        throw new UnsupportedOperationException(s"ManagedBuffer of $mb not supported")
    }
    result
  }
}
