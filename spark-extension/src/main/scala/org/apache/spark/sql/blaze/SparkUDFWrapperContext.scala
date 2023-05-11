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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Nondeterministic
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.execution.blaze.arrowio.ColumnarHelper
import org.apache.spark.sql.execution.blaze.arrowio.util.ArrowUtils
import org.apache.spark.sql.execution.blaze.arrowio.util.ArrowWriter
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

case class SparkUDFWrapperContext(serialized: ByteBuffer) extends Logging {

  private val (expr, javaParamsSchema) = NativeConverters.deserializeExpression({
    val bytes = new Array[Byte](serialized.remaining())
    serialized.get(bytes)
    bytes
  }) match {
    case (nondeterministic: Nondeterministic, paramsSchema) =>
      nondeterministic.initialize(TaskContext.get.partitionId())
      (nondeterministic, paramsSchema)
    case (expr, paramsSchema) =>
      (expr, paramsSchema)
  }

  private val dictionaryProvider: DictionaryProvider = new MapDictionaryProvider()
  private val outputSchema = {
    val schema = StructType(Seq(StructField("", expr.dataType, expr.nullable)))
    ArrowUtils.toArrowSchema(schema)
  }
  private val paramsSchema = ArrowUtils.toArrowSchema(javaParamsSchema)

  def eval(importFFIArrayPtr: Long, exportFFIArrayPtr: Long): Unit = {
    var outputRoot: VectorSchemaRoot = null
    var paramsRoot: VectorSchemaRoot = null

    Utils.tryWithSafeFinally {
      outputRoot = VectorSchemaRoot.create(outputSchema, ArrowUtils.rootAllocator)
      paramsRoot = VectorSchemaRoot.create(paramsSchema, ArrowUtils.rootAllocator)

      // import into params root
      val importArray = ArrowArray.wrap(importFFIArrayPtr)
      Utils.tryWithSafeFinally {
        Data.importIntoVectorSchemaRoot(
          ArrowUtils.rootAllocator,
          importArray,
          paramsRoot,
          dictionaryProvider)
        true
      } {
        importArray.close()
      }

      // evaluate expression and write to output root
      val outputWriter = ArrowWriter.create(outputRoot)
      for (paramsRow <- ColumnarHelper.batchAsRowIter(ColumnarHelper.rootAsBatch(paramsRoot))) {
        val outputRow = InternalRow(expr.eval(paramsRow))
        outputWriter.write(outputRow)
      }
      outputWriter.finish()

      // export to output
      val exportArray = ArrowArray.wrap(exportFFIArrayPtr)
      Utils.tryWithSafeFinally {
        Data.exportVectorSchemaRoot(
          ArrowUtils.rootAllocator,
          outputRoot,
          dictionaryProvider,
          exportArray)
      } {
        exportArray.close()
      }
    } {
      if (outputRoot != null) {
        outputRoot.close()
      }
      if (paramsRoot != null) {
        paramsRoot.close()
      }
    }
  }
}
