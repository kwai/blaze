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
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.SparkEnv
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.blaze.arrowio.util2.ArrowUtils
import org.apache.spark.sql.execution.blaze.arrowio.util2.ArrowWriter
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

class ArrowFFIStreamExporter(
    taskContext: TaskContext,
    batchedRows: Iterator[Iterator[InternalRow]],
    schema: StructType,
    arrowFFIStreamPtr: Long)
    extends Logging {

  private var allocator =
    ArrowUtils.rootAllocator.newChildAllocator("arrowFFIStreamExporter", 0, Long.MaxValue)

  private val arrowReader: ArrowReader = new ArrowReader(allocator) {
    val arrowWriter: ArrowWriter = ArrowWriter.create(getVectorSchemaRoot)

    override def loadNextBatch(): Boolean = {
      if (batchedRows.hasNext) {
        arrowWriter.reset()

        batchedRows
          .next()
          .foreach(row => {
            arrowWriter.write(row)
          })
        arrowWriter.finish()
        return true
      }
      false
    }

    override def bytesRead(): Long = 0L
    override def closeReadSource(): Unit = {}

    override def readSchema(): Schema = {
      val timeZoneId = SparkEnv.get.conf.get(SQLConf.SESSION_LOCAL_TIMEZONE)
      ArrowUtils.toArrowSchema(schema, timeZoneId)
    }
  }

  taskContext.addTaskCompletionListener[Unit](_ => close())

  def exportArrayStream(): Unit = {
    Data.exportArrayStream(allocator, arrowReader, ArrowArrayStream.wrap(arrowFFIStreamPtr))
  }

  private def close(): Unit = {
    synchronized {
      if (allocator != null) {
        arrowReader.close()
        allocator.close()
        allocator = null
      }
    }
  }
}
