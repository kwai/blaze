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
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.blaze.arrowio.util.ArrowUtils
import org.apache.spark.sql.execution.blaze.arrowio.util.ArrowWriter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.blaze.{BlazeConf, NativeHelper}
import org.apache.spark.sql.blaze.util.Using
import org.apache.spark.TaskContext

import java.security.PrivilegedExceptionAction

class ArrowFFIExporter(rowIter: Iterator[InternalRow], schema: StructType) {
  private val maxBatchNumRows = BlazeConf.BATCH_SIZE.intConf()
  private val maxBatchMemorySize = 1 << 24 // 16MB

  private val arrowSchema = ArrowUtils.toArrowSchema(schema)
  private val emptyDictionaryProvider = new MapDictionaryProvider()

  def hasNext: Boolean = {
    val tc = TaskContext.get()
    if (tc != null && (tc.isCompleted() || tc.isInterrupted())) {
      return false
    }
    rowIter.hasNext
  }

  def exportSchema(exportArrowSchemaPtr: Long): Unit = {
    Using.resource(ArrowUtils.newChildAllocator(getClass.getName)) { schemaAllocator =>
      Using.resource(ArrowSchema.wrap(exportArrowSchemaPtr)) { exportSchema =>
        Data.exportSchema(schemaAllocator, arrowSchema, emptyDictionaryProvider, exportSchema)
      }
    }
  }

  def exportNextBatch(exportArrowArrayPtr: Long): Boolean = {
    val tc = TaskContext.get()

    if (tc != null && (tc.isCompleted() || tc.isInterrupted())) return false

    val currentUserInfo = UserGroupInformation.getCurrentUser
    val nativeCurrentUser = NativeHelper.currentUser
    val isNativeCurrentUser = currentUserInfo.equals(nativeCurrentUser)
    // if current user is native user, process rows directly
    if (isNativeCurrentUser) {
      callRowIter(exportArrowArrayPtr)
    } else {
      // otherwise, process rows as native user
      nativeCurrentUser.doAs(new PrivilegedExceptionAction[Boolean] {
        override def run(): Boolean = {
          callRowIter(exportArrowArrayPtr)
        }
      })
    }
  }

  private def callRowIter(exportArrowArrayPtr: Long): Boolean = {
    if (!rowIter.hasNext) return false

    Using.resource(ArrowUtils.newChildAllocator(getClass.getName)) { batchAllocator =>
      Using.resources(
        VectorSchemaRoot.create(arrowSchema, batchAllocator),
        ArrowArray.wrap(exportArrowArrayPtr)) { case (root, exportArray) =>
        val arrowWriter = ArrowWriter.create(root)
        var rowCount = 0

          rowCount += 1
        def processRows(): Unit = {
          while (rowIter.hasNext
            && rowCount < maxBatchNumRows
            && batchAllocator.getAllocatedMemory < maxBatchMemorySize) {
            arrowWriter.write(rowIter.next())
            rowCount += 1
          }
        }
        // if current user is native user, process rows directly
        if (isNativeCurrentUser) {
          processRows()
        } else {
          // otherwise, process rows as native user
          nativeCurrentUser.doAs(new PrivilegedExceptionAction[Unit] {
            override def run(): Unit = {
              processRows()
            }
          })
        }
        arrowWriter.finish()

        // export using root allocator
        Data.exportVectorSchemaRoot(
          ArrowUtils.rootAllocator,
          root,
          emptyDictionaryProvider,
          exportArray)
      }
    }
    true
  }
}
