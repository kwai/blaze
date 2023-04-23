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

import org.apache.spark.InterruptibleIterator
import org.apache.spark.TaskContext

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Nondeterministic
import org.apache.spark.sql.execution.blaze.arrowio.ArrowFFIStreamExporter
import org.apache.spark.sql.execution.blaze.arrowio.ArrowFFIStreamImportIterator
import org.apache.spark.sql.execution.blaze.arrowio.ColumnarHelper
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

case class SparkUDFWrapperContext(
    serialized: ByteBuffer,
    arrowImportFFIStreamPtr: Long,
    arrowExportFFIStreamPtr: Long)
    extends Logging {

  private val _init: Unit = {
    val taskContext = TaskContext.get()
    val expr: Expression = NativeConverters.deserializeExpression({
      val bytes = new Array[Byte](serialized.remaining())
      serialized.get(bytes)
      bytes
    }) match {
      case nondeterministic: Nondeterministic =>
        nondeterministic.initialize(TaskContext.get.partitionId())
        nondeterministic
      case expr =>
        expr
    }

    val importStream = new InterruptibleIterator[ColumnarBatch](
      taskContext,
      new ArrowFFIStreamImportIterator(taskContext, arrowImportFFIStreamPtr))

    val outputRows: Iterator[Iterator[InternalRow]] = importStream.map(batch => {
      val batchedParamRows = ColumnarHelper.batchAsRowIter(batch)
      val batchedResultRows = batchedParamRows.map(row => InternalRow(expr.eval(row)))
      batchedResultRows
    })

    val outputSchema: StructType = StructType(
      StructField("", expr.dataType, expr.nullable) :: Nil)

    val exporter = new ArrowFFIStreamExporter(
      TaskContext.get(),
      outputRows,
      outputSchema,
      arrowExportFFIStreamPtr)
    exporter.exportArrayStream()
  }
}
