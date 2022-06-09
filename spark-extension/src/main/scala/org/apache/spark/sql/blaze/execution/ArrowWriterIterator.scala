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

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.nio.channels.Channels
import java.nio.channels.ReadableByteChannel

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.commons.compress.utils.SeekableInMemoryByteChannel
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util2.ArrowUtils2
import org.apache.spark.sql.util2.ArrowWriter
import org.apache.spark.util.Utils
import org.apache.spark.TaskContext

class ArrowWriterIterator(
    rowIter: Iterator[InternalRow],
    schema: StructType,
    timeZoneId: String,
    taskContext: TaskContext,
    recordBatchSize: Int = 10000)
    extends Iterator[ReadableByteChannel] {

  private val allocator =
    ArrowUtils2.rootAllocator.newChildAllocator("arrowWriterIterator", 0, Long.MaxValue)
  private val arrowSchema = ArrowUtils2.toArrowSchema(schema, timeZoneId)
  private val root = VectorSchemaRoot.create(arrowSchema, allocator)
  private var finished = false

  taskContext.addTaskCompletionListener[Unit] { _ =>
    root.close()
    allocator.close()
  }

  override def hasNext: Boolean =
    !finished && {
      if (!rowIter.hasNext) {
        close()
        return false
      }
      true
    }

  override def next(): ReadableByteChannel = {
    val arrowWriter = ArrowWriter.create(root)
    var rowCount = 0
    while (rowIter.hasNext && rowCount < recordBatchSize) {
      arrowWriter.write(rowIter.next())
      rowCount += 1
    }
    arrowWriter.finish()

    Utils.tryWithResource(new ByteArrayOutputStream()) { outputStream =>
      Utils.tryWithResource(Channels.newChannel(outputStream)) { channel =>
        val writer = new ArrowStreamWriter(root, new MapDictionaryProvider(), channel)
        writer.start()
        writer.writeBatch()
        writer.end()
        writer.close()
      }
      root.clear()
      new SeekableInMemoryByteChannel(outputStream.toByteArray)
    }
  }

  private def close(): Unit =
    synchronized {
      if (!finished) {
        root.close()
        allocator.close()
        finished = true
      }
    }
}
