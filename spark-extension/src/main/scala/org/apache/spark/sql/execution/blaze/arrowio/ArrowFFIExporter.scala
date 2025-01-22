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
import org.apache.spark.sql.blaze.{BlazeConf, NativeHelper}
import org.apache.spark.sql.blaze.util.Using
import org.apache.spark.TaskContext
import java.security.PrivilegedExceptionAction
import java.util.concurrent.BlockingQueue
import java.util.concurrent.SynchronousQueue

class ArrowFFIExporter(rowIter: Iterator[InternalRow], schema: StructType)
  extends AutoCloseable {

  private val maxBatchNumRows = BlazeConf.BATCH_SIZE.intConf()
  private val maxBatchMemorySize = 1 << 24 // 16MB

  private val arrowSchema = ArrowUtils.toArrowSchema(schema)
  private val emptyDictionaryProvider = new MapDictionaryProvider()
  private val nativeCurrentUser = NativeHelper.currentUser

  private trait QueueElement
  private case class Root(root: VectorSchemaRoot) extends QueueElement
  private case object RootConsumed extends QueueElement
  private case object Finished extends QueueElement

  private val tc = TaskContext.get()
  private val outputQueue: BlockingQueue[QueueElement] = new SynchronousQueue[QueueElement]()
  private var currentRoot: VectorSchemaRoot = _
  private var finished = false
  private val outputThread = startOutputThread()

  def exportSchema(exportArrowSchemaPtr: Long): Unit = {
    Using.resource(ArrowUtils.newChildAllocator(getClass.getName)) { schemaAllocator =>
      Using.resource(ArrowSchema.wrap(exportArrowSchemaPtr)) { exportSchema =>
        Data.exportSchema(schemaAllocator, arrowSchema, emptyDictionaryProvider, exportSchema)
      }
    }
  }

  def exportNextBatch(exportArrowArrayPtr: Long): Boolean = {
    if (!hasNext) {
      return false
    }

    // export using root allocator
    val allocator = ArrowUtils.rootAllocator
    Using.resource(ArrowArray.wrap(exportArrowArrayPtr)) { exportArray =>
      Data.exportVectorSchemaRoot(allocator, currentRoot, emptyDictionaryProvider, exportArray)
    }

    // consume RootConsumed state so that we can go to the next batch
    outputQueue.take() match {
      case RootConsumed =>
      case other =>
        throw new IllegalStateException(s"Unexpected queue element: $other, expect RootConsumed")
    }
    true
  }

  private def hasNext: Boolean = {
    if (tc != null && (tc.isCompleted() || tc.isInterrupted())) {
      finished = true
      return false
    }

    outputQueue.take() match {
      case Root(root) =>
        currentRoot = root
        true
      case Finished =>
        currentRoot = null
        finished = true
        false
      case other =>
        throw new IllegalStateException(s"Unexpected queue element: $other, expect Root/Finished")
    }
  }

  private def startOutputThread(): Thread = {
    val thread = new Thread(new Runnable {
      override def run(): Unit = {
        if (tc != null) {
          TaskContext.setTaskContext(tc)
        }

        nativeCurrentUser.doAs(new PrivilegedExceptionAction[Unit] {
          override def run(): Unit = {
            while (!finished && (tc == null || (!tc.isCompleted() && !tc.isInterrupted()))) {
              if (!rowIter.hasNext) {
                finished = true
                outputQueue.put(Finished)
                return
              }

              Using.resource(ArrowUtils.newChildAllocator(getClass.getName)) { batchAllocator =>
                Using.resource(VectorSchemaRoot.create(arrowSchema, batchAllocator)) { root =>
                  val arrowWriter = ArrowWriter.create(root)
                  var rowCount = 0

                  while (rowIter.hasNext
                    && rowCount < maxBatchNumRows
                    && batchAllocator.getAllocatedMemory < maxBatchMemorySize) {
                    arrowWriter.write(rowIter.next())
                    rowCount += 1
                  }
                  arrowWriter.finish()

                  // export root
                  outputQueue.put(Root(root))
                  outputQueue.put(RootConsumed)
                }
              }
            }
          }
        })
      }
    })

    if (tc != null) {
      tc.addTaskCompletionListener[Unit]((_: TaskContext) => close())
      tc.addTaskFailureListener((_, _) => close())
    }
    thread.setDaemon(true)
    thread.start()
    thread
  }

  override def close(): Unit = {
    finished = true
    outputThread.interrupt()
    outputQueue.offer(Finished) // to abort any pending call to exportNextBatch()
  }
}
