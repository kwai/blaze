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

import java.nio.channels.SeekableByteChannel
import java.nio.ByteBuffer

import org.apache.arrow.vector.ipc.ArrowFileReader
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.TaskContext
import org.apache.spark.sql.blaze.FFIHelper
import org.apache.spark.sql.util2.ArrowUtils2

class ArrowReaderIterator(ipc: IpcData, taskContext: TaskContext) extends Iterator[InternalRow] {

  // decompress ipc into memory
  private val channel: SeekableByteChannel = {
    val buf = new Array[Byte](ipc.ipcLengthUncompressed.toInt)
    ipc.readArrowData(ByteBuffer.wrap(buf))
    new ByteArrayReadableSeekableByteChannel(buf)
  }

  private val allocator =
    ArrowUtils2.rootAllocator.newChildAllocator("arrowReaderIterator", 0, Long.MaxValue)
  private val arrowReader = new ArrowFileReader(channel, allocator)
  private val root = arrowReader.getVectorSchemaRoot
  private var closed = false
  private var rowIter = nextBatch()

  taskContext.addTaskCompletionListener[Unit] { _ =>
    if (!closed) {
      root.close()
      allocator.close()
      arrowReader.close()
      closed = true
    }
  }

  override def hasNext: Boolean =
    rowIter.hasNext || {
      rowIter = nextBatch()
      rowIter.nonEmpty
    }

  override def next(): InternalRow = rowIter.next()

  private def nextBatch(): Iterator[InternalRow] = {
    if (arrowReader.loadNextBatch()) {
      FFIHelper.batchAsRowIter(FFIHelper.rootAsBatch(root))
    } else {
      Iterator.empty
    }
  }
}
