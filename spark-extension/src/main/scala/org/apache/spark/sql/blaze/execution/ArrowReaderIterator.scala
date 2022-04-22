package org.apache.spark.sql.blaze.execution

import java.nio.channels.SeekableByteChannel
import org.apache.arrow.vector.ipc.ArrowFileReader
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.TaskContext
import org.apache.spark.sql.blaze.FFIHelper.rootAsRowIter
import org.apache.spark.sql.util2.ArrowUtils2

class ArrowReaderIterator(channel: SeekableByteChannel, taskContext: TaskContext) {
  private val allocator =
    ArrowUtils2.rootAllocator.newChildAllocator("arrowReaderIterator", 0, Long.MaxValue)
  private val arrowReader = new ArrowFileReader(channel, allocator)
  private val root = arrowReader.getVectorSchemaRoot
  private var closed = false

  val result: Iterator[InternalRow] = new Iterator[InternalRow] {
    private var rowIter = nextBatch()

    taskContext.addTaskCompletionListener[Unit] { _ =>
      if (!closed) {
        root.close()
        allocator.close()
        arrowReader.close()
        closed = true
      }
    }

    override def hasNext: Boolean = rowIter.hasNext || {
      rowIter = nextBatch()
      rowIter.nonEmpty
    }

    override def next(): InternalRow = rowIter.next()

    private def nextBatch(): Iterator[InternalRow] = {
      if (arrowReader.loadNextBatch()) {
        rootAsRowIter(root)
      } else {
        Iterator.empty
      }
    }
  }
}
