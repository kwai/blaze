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

import org.apache.arrow.c.ArrowArray
import org.apache.arrow.c.ArrowSchema
import org.apache.arrow.c.Data
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.blaze.arrowio.util.ArrowUtils
import org.apache.spark.sql.execution.blaze.arrowio.util.ArrowWriter
import org.apache.spark.sql.types.StructType
import org.apache.spark.internal.Logging
import org.apache.spark.sql.blaze.BlazeConf
import org.apache.spark.sql.blaze.util.Using

class ArrowFFIExportIterator(rowIter: Iterator[InternalRow], schema: StructType)
    extends Iterator[(Long, Long) => Unit]
    with Logging {

  // NOTE:
  private val maxBatchNumRows = BlazeConf.BATCH_SIZE.intConf()
  private val maxBatchMemorySize = 1 << 24 // 16MB

  private val arrowSchema = ArrowUtils.toArrowSchema(schema)
  private val emptyDictionaryProvider = new MapDictionaryProvider()
  private var currentVectorConsumed = true

  override def hasNext: Boolean = {
    assert(currentVectorConsumed)
    rowIter.hasNext
  }

  override def next(): (Long, Long) => Unit = {
    (exportArrowSchemaPtr: Long, exportArrowArrayPtr: Long) =>
      {
        Using.resource(ArrowUtils.newChildAllocator(getClass.getName)) { batchAllocator =>
          currentVectorConsumed = false

          Using.resources(
            VectorSchemaRoot.create(arrowSchema, batchAllocator),
            ArrowArray.wrap(exportArrowArrayPtr),
            ArrowSchema.wrap(exportArrowSchemaPtr)) { case (root, exportArray, exportSchema) =>
            val arrowWriter = ArrowWriter.create(root)
            var rowCount = 0

            while (rowIter.hasNext
              && rowCount < maxBatchNumRows
              && batchAllocator.getAllocatedMemory < maxBatchMemorySize) {
              arrowWriter.write(rowIter.next())
              rowCount += 1
            }
            arrowWriter.finish()
            currentVectorConsumed = true

            // export using root allocator
            Data.exportVectorSchemaRoot(
              ArrowUtils.rootAllocator,
              root,
              emptyDictionaryProvider,
              exportArray,
              exportSchema)
          }
        }
      }
  }
}
