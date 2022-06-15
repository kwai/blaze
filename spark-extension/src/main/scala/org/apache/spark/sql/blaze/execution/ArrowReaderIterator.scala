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

import java.nio.channels.ReadableByteChannel

import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.TaskContext
import org.apache.spark.sql.blaze.FFIHelper
import org.apache.spark.sql.util2.ArrowUtils2

class ArrowReaderIterator(channel: ReadableByteChannel, taskContext: TaskContext)
    extends Iterator[InternalRow] {

  private var allocator =
    ArrowUtils2.rootAllocator.newChildAllocator("arrowReaderIterator", 0, Long.MaxValue)
  private var arrowReader = new ArrowStreamReader(channel, allocator)
  private var root = arrowReader.getVectorSchemaRoot
  private var rowIter = nextBatch()

  taskContext.addTaskCompletionListener[Unit](_ => close())

  override def hasNext: Boolean =
    (root != null && rowIter.hasNext) || {
      rowIter = nextBatch()
      if (rowIter.isEmpty) {
        close()
        return false
      }
      true
    }

  override def next(): InternalRow = rowIter.next()

  private def nextBatch(): Iterator[InternalRow] = {
    if (arrowReader.loadNextBatch()) {
      FFIHelper.batchAsRowIter(FFIHelper.rootAsBatch(root))
    } else {
      Iterator.empty
    }
  }

  private def close(): Unit =
    synchronized {
      if (root != null) {
        root.close()
        allocator.close()
        arrowReader.close()
        root = null
        allocator = null
        arrowReader = null
      }
    }
}
