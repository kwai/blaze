/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.blaze

import scala.collection.JavaConverters._

import org.apache.arrow.c.ArrowArray
import org.apache.arrow.c.ArrowSchema
import org.apache.arrow.c.CDataDictionaryProvider
import org.apache.arrow.c.Data
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.util2.ArrowColumnVector
import org.apache.spark.sql.util2.ArrowUtils2
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.vectorized.ColumnVector
import org.apache.spark.util.CompletionIterator

object FFIHelper {
  def tryWithResource[R <: AutoCloseable, T](createResource: => R)(f: R => T): T = {
    val resource = createResource
    try f.apply(resource)
    finally resource.close()
  }

  def rootAsRowIter(root: VectorSchemaRoot): Iterator[InternalRow] = {
    val columns = root.getFieldVectors.asScala.map { vector =>
      new ArrowColumnVector(vector).asInstanceOf[ColumnVector]
    }.toArray
    val batch = new ColumnarBatch(columns)
    batch.setNumRows(root.getRowCount)

    CompletionIterator[InternalRow, Iterator[InternalRow]](
      batch.rowIterator().asScala, {
        batch.close()
      })
  }

  def fromBlazeIter(
      iterPtr: Long,
      context: TaskContext,
      metrics: MetricNode): Iterator[InternalRow] = {
    val allocator =
      ArrowUtils2.rootAllocator.newChildAllocator("fromBLZIterator", 0, Long.MaxValue)
    val provider = new CDataDictionaryProvider()

    val root = tryWithResource(ArrowSchema.allocateNew(allocator)) { consumerSchema =>
      tryWithResource(ArrowArray.allocateNew(allocator)) { consumerArray =>
        val schemaPtr: Long = consumerSchema.memoryAddress
        val arrayPtr: Long = consumerArray.memoryAddress
        val rt = JniBridge.loadNext(iterPtr, schemaPtr, arrayPtr)
        if (rt < 0) {
          return Iterator.empty
        }
        val root: VectorSchemaRoot =
          Data.importVectorSchemaRoot(allocator, consumerArray, consumerSchema, provider)
        root
      }
    }

    new Iterator[InternalRow] {
      private val metricsToUpdate = metrics
      private var rowIter = rootAsRowIter(root)
      private var finished = false

      context.addTaskCompletionListener[Unit] { _ =>
        finish()
      }

      override def hasNext: Boolean =
        rowIter.hasNext || {
          rowIter = nextBatch()
          rowIter match {
            case rowIter if rowIter.nonEmpty => true
            case _ =>
              finish()
              false
          }
        }

      override def next(): InternalRow = rowIter.next()

      private def nextBatch(): Iterator[InternalRow] = {
        tryWithResource(ArrowSchema.allocateNew(allocator)) { consumerSchema =>
          tryWithResource(ArrowArray.allocateNew(allocator)) { consumerArray =>
            val schemaPtr: Long = consumerSchema.memoryAddress
            val arrayPtr: Long = consumerArray.memoryAddress
            val rt = JniBridge.loadNext(iterPtr, schemaPtr, arrayPtr)
            if (rt < 0) {
              return Iterator.empty
            }

            Data.importIntoVectorSchemaRoot(allocator, consumerArray, root, provider)
            rootAsRowIter(root)
          }
        }
      }

      private def finish(): Unit = {
        if (!finished) {
          finished = true
          JniBridge.updateMetrics(iterPtr, metricsToUpdate)
          JniBridge.deallocIter(iterPtr)
          root.close()
          allocator.close()
        }
      }
    }
  }
}
