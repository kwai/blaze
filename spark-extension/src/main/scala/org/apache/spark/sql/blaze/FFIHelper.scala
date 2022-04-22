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

    CompletionIterator[InternalRow, Iterator[InternalRow]](batch.rowIterator().asScala, {
      batch.close()
    })
  }

  def fromBlazeIter(iterPtr: Long, context: TaskContext): Iterator[InternalRow] = {
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
      private var rowIter = rootAsRowIter(root)

      context.addTaskCompletionListener[Unit] { _ =>
        root.close()
        allocator.close()
      }

      override def hasNext: Boolean = rowIter.hasNext || {
        rowIter = nextBatch()
        rowIter.nonEmpty
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
    }
  }
}
