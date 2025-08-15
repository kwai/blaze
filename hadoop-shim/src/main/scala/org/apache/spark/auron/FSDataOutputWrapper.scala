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

import org.apache.hadoop.fs.FSDataOutputStream

trait FSDataOutputWrapper extends AutoCloseable {
  def writeFully(buf: ByteBuffer): Unit
}

object FSDataOutputWrapper {
  def wrap(output: FSDataOutputStream): FSDataOutputWrapper = {
    new SeekableFSDataOutputWrapper(output)
  }
}

class SeekableFSDataOutputWrapper(output: FSDataOutputStream) extends FSDataOutputWrapper {
  override def writeFully(buf: ByteBuffer): Unit = {
    output.synchronized {
      val channel = Channels.newChannel(output)
      while (buf.hasRemaining) if (channel.write(buf) == -1) {
        throw new EOFException("writeFullyToFSDataOutputStream() got unexpected EOF")
      }
    }
  }

  override def close(): Unit = output.close()
}
