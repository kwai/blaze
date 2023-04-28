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

import org.apache.arrow.c.ArrowArrayStream
import org.apache.arrow.c.Data
import org.apache.spark.TaskContext

import org.apache.spark.sql.execution.blaze.arrowio.util.ArrowUtils
import org.apache.spark.sql.vectorized.ColumnarBatch

class ArrowFFIStreamImportIterator(taskContext: TaskContext, arrowFFIStreamPtr: Long)
    extends Iterator[ColumnarBatch] {
  private var stream = ArrowArrayStream.wrap(arrowFFIStreamPtr)
  private var reader = Data.importArrayStream(ArrowUtils.rootAllocator, stream)
  private var currentBatchNotConsumed = false

  taskContext.addTaskCompletionListener[Unit](_ => close())

  override def hasNext: Boolean =
    synchronized {
      if (currentBatchNotConsumed) {
        return true
      }

      try {
        currentBatchNotConsumed = reader.loadNextBatch()
      } catch {
        case _ if taskContext.isCompleted() || taskContext.isInterrupted() =>
          currentBatchNotConsumed = false
      }
      currentBatchNotConsumed
    }

  override def next(): ColumnarBatch = {
    currentBatchNotConsumed = false
    ColumnarHelper.rootAsBatch(reader.getVectorSchemaRoot)
  }

  def close(): Unit =
    synchronized {
      if (stream != null) {
        reader.close()
        reader = null
        stream.close()
        stream = null
      }
    }
}
