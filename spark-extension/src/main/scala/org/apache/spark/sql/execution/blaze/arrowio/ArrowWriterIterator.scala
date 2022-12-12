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

import java.io.ByteArrayOutputStream
import java.nio.channels.Channels
import java.nio.channels.ReadableByteChannel

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.commons.compress.utils.SeekableInMemoryByteChannel
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.execution.blaze.arrowio.util2.ArrowUtils
import org.apache.spark.sql.execution.blaze.arrowio.util2.ArrowWriter
import org.apache.spark.util.Utils
import org.apache.spark.TaskContext
import org.apache.spark.sql.blaze.NativeHelper

class ArrowWriterIterator(
    rowIter: Iterator[InternalRow],
    schema: StructType,
    timeZoneId: String,
    taskContext: TaskContext,
    recordBatchSize: Int = NativeHelper.batchSize)
    extends Iterator[ReadableByteChannel] {

  private val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId)
  private var allocator =
    ArrowUtils.rootAllocator.newChildAllocator("arrowWriterIterator", 0, Long.MaxValue)
  private val emptyDictionaryProvider = new MapDictionaryProvider()

  if (taskContext != null) {
    taskContext.addTaskCompletionListener[Unit](_ => close())
  }

  override def hasNext: Boolean = allocator != null && rowIter.hasNext

  override def next(): ReadableByteChannel = {
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    val arrowWriter = ArrowWriter.create(root)
    var rowCount = 0

    while (rowIter.hasNext && rowCount < recordBatchSize) {
      arrowWriter.write(rowIter.next())
      rowCount += 1
    }
    arrowWriter.finish()

    Utils.tryWithResource(new ByteArrayOutputStream()) { outputStream =>
      Utils.tryWithResource(Channels.newChannel(outputStream)) { channel =>
        val writer = new ArrowStreamWriter(root, emptyDictionaryProvider, channel)
        writer.start()
        writer.writeBatch()
        writer.end()
        writer.close()
      }
      root.close()
      new SeekableInMemoryByteChannel(outputStream.toByteArray)
    }
  }

  def close(): Unit =
    synchronized {
      if (allocator != null) {
        allocator.close()
        allocator = null
      }
    }
}
