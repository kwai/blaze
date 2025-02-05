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

import scala.collection.JavaConverters._
import org.apache.arrow.c.{ArrowArray, Data}
import org.apache.arrow.vector.{IntVector, VectorSchemaRoot}
import org.apache.arrow.vector.dictionary.DictionaryProvider
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.blaze.util.Using
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.catalyst.expressions.Nondeterministic
import org.apache.spark.sql.execution.blaze.arrowio.ColumnarHelper
import org.apache.spark.sql.execution.blaze.arrowio.util.{ArrowUtils, ArrowWriter}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.catalyst.expressions.{Attribute, JoinedRow, MutableProjection}
import org.apache.spark.sql.catalyst.expressions.AttributeReference

import java.nio.ByteBuffer

case class SparkUDAFWrapperContext(serialized: ByteBuffer) extends Logging {
  private val (expr, javaParamsSchema) =
    NativeConverters.deserializeExpression[DeclarativeAggregate]({
      val bytes = new Array[Byte](serialized.remaining())
      serialized.get(bytes)
      bytes
    })

  val inputAttributes: Seq[Attribute] = javaParamsSchema.fields.map { field =>
    AttributeReference(field.name, field.dataType, field.nullable)()
  }

  // initialize all nondeterministic children exprs
  expr.foreach {
    case nondeterministic: Nondeterministic =>
      nondeterministic.initialize(TaskContext.get.partitionId())
    case _ =>
  }

  private lazy val initializer = MutableProjection.create(expr.initialValues)

  private lazy val updater =
    MutableProjection.create(expr.updateExpressions, expr.aggBufferAttributes ++ inputAttributes)

  private lazy val merger = MutableProjection.create(
    expr.mergeExpressions,
    expr.aggBufferAttributes ++ expr.inputAggBufferAttributes)

  private lazy val evaluator =
    MutableProjection.create(expr.evaluateExpression :: Nil, expr.aggBufferAttributes)

  private def initialize(): InternalRow = initializer.apply(InternalRow.empty).copy()

  private val dictionaryProvider: DictionaryProvider = new MapDictionaryProvider()

  private val inputSchema = ArrowUtils.toArrowSchema(javaParamsSchema)

  {
    val schema = StructType(Seq(StructField("", expr.dataType, expr.nullable)))
    ArrowUtils.toArrowSchema(schema)
  }

  private val indexSchema = {
    val schema = StructType(Seq(StructField("", LongType), StructField("", LongType)))
    ArrowUtils.toArrowSchema(schema)
  }

  def update(
      rows: Array[InternalRow],
      importIdxFFIArrayPtr: Long,
      importBatchFFIArrayPtr: Long): Array[InternalRow] = {
    Using.resource(ArrowUtils.newChildAllocator(getClass.getName)) { batchAllocator =>
      Using.resources(
        VectorSchemaRoot.create(inputSchema, batchAllocator),
        VectorSchemaRoot.create(indexSchema, batchAllocator),
        ArrowArray.wrap(importIdxFFIArrayPtr),
        ArrowArray.wrap(importBatchFFIArrayPtr)) { (inputRoot, idxRoot, inputArray, idxArray) =>
        // import into params root
        Data.importIntoVectorSchemaRoot(batchAllocator, inputArray, inputRoot, dictionaryProvider)
        val batch = ColumnarHelper.rootAsBatch(inputRoot)
        val inputRows = ColumnarHelper.batchAsRowIter(batch).toArray

        Data.importIntoVectorSchemaRoot(batchAllocator, idxArray, idxRoot, dictionaryProvider)
        val fieldVectors = idxRoot.getFieldVectors.asScala
        val rowIdxVector = fieldVectors.head.asInstanceOf[IntVector]
        val inputIdxVector = fieldVectors(1).asInstanceOf[IntVector]

        for (i <- 0 until idxRoot.getRowCount) {
          val row = rows(rowIdxVector.get(i))
          val input = inputRows(inputIdxVector.get(i))
          val joiner = new JoinedRow
          if (row.numFields == 0) {
            rows(rowIdxVector.get(i)) = updater(joiner(initialize(), input))
          } else {
            rows(rowIdxVector.get(i)) = updater(joiner(row, input))
          }
        }

        rows
      }
    }
  }

  def merge(
      rows: Array[InternalRow],
      mergeRows: Array[InternalRow],
      importIdxFFIArrayPtr: Long): Array[InternalRow] = {
    Using.resource(ArrowUtils.newChildAllocator(getClass.getName)) { batchAllocator =>
      Using.resources(
        VectorSchemaRoot.create(indexSchema, batchAllocator),
        ArrowArray.wrap(importIdxFFIArrayPtr)) { (idxRoot, idxArray) =>
        Data.importIntoVectorSchemaRoot(batchAllocator, idxArray, idxRoot, dictionaryProvider)
        val fieldVectors = idxRoot.getFieldVectors.asScala
        val rowIdxVector = fieldVectors.head.asInstanceOf[IntVector]
        val mergeIdxVector = fieldVectors(1).asInstanceOf[IntVector]

        for (i <- 0 until idxRoot.getRowCount) {
          val row = rows(rowIdxVector.get(i))
          val mergeRow = mergeRows(mergeIdxVector.get(i))
          val joiner = new JoinedRow
          if (row.numFields == 0) {
            rows(rowIdxVector.get(i)) = merger(joiner(initialize(), mergeRow))
          } else {
            rows(rowIdxVector.get(i)) = merger(joiner(row, mergeRow))
          }
        }

        rows
      }
    }
  }

  def eval(
      rows: Array[InternalRow],
      importIdxFFIArrayPtr: Long,
      exportFFIArrayPtr: Long): Unit = {
    Using.resource(ArrowUtils.newChildAllocator(getClass.getName)) { batchAllocator =>
      Using.resources(
        VectorSchemaRoot.create(indexSchema, batchAllocator),
        VectorSchemaRoot.create(inputSchema, batchAllocator),
        ArrowArray.wrap(importIdxFFIArrayPtr),
        ArrowArray.wrap(exportFFIArrayPtr)) { (idxRoot, outputRoot, idxArray, exportArray) =>
        Data.importIntoVectorSchemaRoot(batchAllocator, idxArray, idxRoot, dictionaryProvider)
        val fieldVectors = idxRoot.getFieldVectors.asScala
        val rowIdxVector = fieldVectors.head.asInstanceOf[IntVector]

        // evaluate expression and write to output root
        val outputWriter = ArrowWriter.create(outputRoot)
        for (i <- 0 until idxRoot.getRowCount) {
          val row = rows(rowIdxVector.get(i))
          outputWriter.write(evaluator(row))
        }
        outputWriter.finish()

        // export to output using root allocator
        Data.exportVectorSchemaRoot(
          ArrowUtils.rootAllocator,
          outputRoot,
          dictionaryProvider,
          exportArray)
      }
    }
  }
}
