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
package org.apache.spark.sql.blaze

import java.nio.ByteBuffer

import org.apache.arrow.c.ArrowArray
import org.apache.arrow.c.Data
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.dictionary.DictionaryProvider
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.blaze.util.Using
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Generator
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.expressions.Nondeterministic
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.execution.blaze.arrowio.util.ArrowUtils
import org.apache.spark.sql.execution.blaze.arrowio.util.ArrowUtils.ROOT_ALLOCATOR
import org.apache.spark.sql.execution.blaze.arrowio.util.ArrowWriter
import org.apache.spark.sql.execution.blaze.columnar.ColumnarHelper
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

case class SparkUDTFWrapperContext(serialized: ByteBuffer) extends Logging {
  private val (expr, javaParamsSchema) = NativeConverters.deserializeExpression[Generator]({
    val bytes = new Array[Byte](serialized.remaining())
    serialized.get(bytes)
    bytes
  })

  // initialize all nondeterministic children exprs
  expr.foreach {
    case nondeterministic: Nondeterministic =>
      nondeterministic.initialize(TaskContext.get.partitionId())
    case _ =>
  }

  private val dictionaryProvider: DictionaryProvider = new MapDictionaryProvider()
  private val javaOutputSchema = StructType(
    Seq(
      StructField("rowid", IntegerType, nullable = false),
      StructField("element", expr.elementSchema, nullable = false)))
  private val outputSchema = ArrowUtils.toArrowSchema(javaOutputSchema)
  private val paramsSchema = ArrowUtils.toArrowSchema(javaParamsSchema)

  def eval(importFFIArrayPtr: Long, exportFFIArrayPtr: Long): Unit = {
    Using.resources(
      VectorSchemaRoot.create(outputSchema, ROOT_ALLOCATOR),
      VectorSchemaRoot.create(paramsSchema, ROOT_ALLOCATOR),
      ArrowArray.wrap(importFFIArrayPtr),
      ArrowArray.wrap(exportFFIArrayPtr)) { (outputRoot, paramsRoot, importArray, exportArray) =>
      // import into params root
      Data.importIntoVectorSchemaRoot(ROOT_ALLOCATOR, importArray, paramsRoot, dictionaryProvider)

      // evaluate expression and write to output root
      val reusedOutputRow = new GenericInternalRow(Array[Any](null, null))
      val outputWriter = ArrowWriter.create(outputRoot)
      for ((paramsRow, rowId) <- ColumnarHelper.rootRowsIter(paramsRoot).zipWithIndex) {
        for (outputRow <- expr.eval(paramsRow)) {
          reusedOutputRow.setInt(0, rowId)
          reusedOutputRow.update(1, outputRow)
          outputWriter.write(reusedOutputRow)
        }
      }
      outputWriter.finish()

      // export to output using root allocator
      Data.exportVectorSchemaRoot(ROOT_ALLOCATOR, outputRoot, dictionaryProvider, exportArray)
    }
  }

  def terminate(rowId: Int, exportFFIArrayPtr: Long): Unit = {
    Using.resources(
      VectorSchemaRoot.create(outputSchema, ROOT_ALLOCATOR),
      ArrowArray.wrap(exportFFIArrayPtr)) { (outputRoot, exportArray) =>
      // evaluate expression and write to output root
      val reusedOutputRow = new GenericInternalRow(Array[Any](null, null))
      val outputWriter = ArrowWriter.create(outputRoot)
      for (outputRow <- expr.terminate()) {
        reusedOutputRow.setInt(0, rowId)
        reusedOutputRow.update(1, outputRow)
        outputWriter.write(reusedOutputRow)
      }
      outputWriter.finish()

      // export to output using root allocator
      Data.exportVectorSchemaRoot(ROOT_ALLOCATOR, outputRoot, dictionaryProvider, exportArray)
    }
  }
}
