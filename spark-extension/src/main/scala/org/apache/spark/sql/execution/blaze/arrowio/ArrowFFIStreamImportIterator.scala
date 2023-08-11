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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.blaze.arrowio.util.ArrowUtils
import org.apache.spark.sql.vectorized.ColumnarBatch

class ArrowFFIStreamImportIterator(
    taskContext: Option[TaskContext],
    arrowFFIStreamPtr: Long,
    onNextBatch: ColumnarBatch => Unit = _ => Unit)
    extends Iterator[InternalRow] {

  private var stream = ArrowArrayStream.wrap(arrowFFIStreamPtr)
  private var reader = Data.importArrayStream(ArrowUtils.rootAllocator, stream)
  private var currentRows: Iterator[InternalRow] = Iterator.empty

  taskContext.foreach(_.addTaskCompletionListener[Unit](_ => close()))

  override def hasNext: Boolean = {
    if (stream == null) { // closed?
      return false
    }
    if (currentRows.hasNext) {
      return true
    }

    // load next batch
    var hasNextBatch = false
    try {
      hasNextBatch = reader.loadNextBatch()
    } catch {
      case _ if taskContext.exists(tc => tc.isCompleted() || tc.isInterrupted()) =>
        hasNextBatch = false
    }
    if (!hasNextBatch) {
      close()
      return false
    }

    val currentBatch = ColumnarHelper.rootAsBatch(reader.getVectorSchemaRoot)
    try {
      onNextBatch(currentBatch)

      // convert batch to persisted row iterator
      val toUnsafe = this.toUnsafe
      val rowIterator = currentBatch.rowIterator()
      val currentRowsArray = new Array[UnsafeRow](currentBatch.numRows())
      var i = 0
      while (rowIterator.hasNext) {
        currentRowsArray(i) = toUnsafe(rowIterator.next()).copy()
        i += 1
      }
      currentRows = currentRowsArray.iterator

    } finally {
      // current batch can be closed after all rows converted to UnsafeRow
      currentBatch.close()
      reader.getVectorSchemaRoot.clear()
    }
    hasNext
  }

  override def next(): InternalRow = {
    currentRows.next()
  }

  private lazy val toUnsafe: UnsafeProjection = {
    val localOutput = ArrowUtils
      .fromArrowSchema(reader.getVectorSchemaRoot.getSchema)
      .map(field => AttributeReference(field.name, field.dataType, field.nullable)())

    val toUnsafe = UnsafeProjection.create(localOutput, localOutput)
    toUnsafe.initialize(taskContext.map(_.partitionId()).getOrElse(0))
    toUnsafe
  }

  def close(): Unit = {
    if (stream != null) {
      currentRows = Iterator.empty
      reader.close()
      reader = null
      stream.close()
      stream = null
    }
  }
}
