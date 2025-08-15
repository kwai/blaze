/*
 * Copyright 2022 The Auron Authors
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
package org.apache.spark.auron

import java.io.EOFException
import java.nio.ByteBuffer
import java.nio.channels.Channels

import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.StreamCapabilities

trait FSDataInputWrapper extends AutoCloseable {
  def readFully(pos: Long, buf: ByteBuffer): Unit
}

object FSDataInputWrapper {
  def wrap(input: FSDataInputStream): FSDataInputWrapper = {
    if (canUsePositionedReadable(input)) {
      new PositionedReadableFSDataInputWrapper(input)
    } else {
      new SeekableFSDataInputWrapper(input)
    }
  }

  private def canUsePositionedReadable(input: FSDataInputStream): Boolean = {
    try {
      input.getClass.getMethod("readFully", classOf[Long], classOf[ByteBuffer]) != null &&
      input.hasCapability(StreamCapabilities.PREADBYTEBUFFER)
    } catch {
      case _: Throwable => false
    }
  }
}

class PositionedReadableFSDataInputWrapper(input: FSDataInputStream) extends FSDataInputWrapper {
  override def readFully(pos: Long, buf: ByteBuffer): Unit = {
    input.readFully(pos, buf)
    if (buf.hasRemaining) {
      throw new EOFException(s"cannot read more ${buf.remaining()} bytes")
    }
  }

  override def close(): Unit = input.close()
}

class SeekableFSDataInputWrapper(input: FSDataInputStream) extends FSDataInputWrapper {
  override def readFully(pos: Long, buf: ByteBuffer): Unit = {
    input.synchronized {
      input.seek(pos)
      while (buf.hasRemaining) {
        val channel = Channels.newChannel(input)
        if (channel.read(buf) == -1) {
          throw new EOFException(s"cannot read more ${buf.remaining()} bytes")
        }
      }
    }
  }

  override def close(): Unit = input.close()
}
