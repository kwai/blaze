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
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, JoinedRow, MutableProjection, Nondeterministic, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.blaze.arrowio.ColumnarHelper
import org.apache.spark.sql.execution.blaze.arrowio.util.{ArrowUtils, ArrowWriter}
import org.apache.spark.sql.types.{DataType, IntegerType, StructField, StructType}
import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer

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

  private lazy val initializer = UnsafeProjection.create(expr.initialValues)

  private lazy val updater =
    UnsafeProjection.create(expr.updateExpressions, expr.aggBufferAttributes ++ inputAttributes)

  private lazy val merger = UnsafeProjection.create(
    expr.mergeExpressions,
    expr.aggBufferAttributes ++ expr.inputAggBufferAttributes)

  private lazy val evaluator =
    UnsafeProjection.create(expr.evaluateExpression :: Nil, expr.aggBufferAttributes)

  private val dictionaryProvider: DictionaryProvider = new MapDictionaryProvider()

  private val inputSchema = ArrowUtils.toArrowSchema(javaParamsSchema)
  private val paramsToUnsafe = {
    val toUnsafe = UnsafeProjection.create(javaParamsSchema)
    toUnsafe.initialize(Option(TaskContext.get()).map(_.partitionId()).getOrElse(0))
    toUnsafe
  }


  private val indexSchema = {
    val schema = StructType(Seq(StructField("", IntegerType), StructField("", IntegerType)))
    ArrowUtils.toArrowSchema(schema)
  }

  private val evalIndexSchema = {
    val schema = StructType(Seq(StructField("", IntegerType)))
    ArrowUtils.toArrowSchema(schema)
  }

  val dataTypes: Seq[DataType] = expr.aggBufferAttributes.map(_.dataType)

  def initialize(numRow: Int): ArrayBuffer[InternalRow] = {
    val rows = ArrayBuffer[InternalRow]()
    resize(rows, numRow)
    rows
  }

  def resize(rows: ArrayBuffer[InternalRow], len: Int): Unit = {
    if (rows.length < len) {
      rows.append(Range(rows.length, len).map(_ => initializer.apply(InternalRow.empty)) :_*)
    } else {
      rows.trimEnd(rows.length - len)
    }
  }

  def update(
      rows: ArrayBuffer[InternalRow],
      importIdxFFIArrayPtr: Long,
      importBatchFFIArrayPtr: Long): Unit = {
    Using.resource(ArrowUtils.newChildAllocator(getClass.getName)) { batchAllocator =>
      Using.resources(
        VectorSchemaRoot.create(inputSchema, batchAllocator),
        VectorSchemaRoot.create(indexSchema, batchAllocator),
        ArrowArray.wrap(importBatchFFIArrayPtr),
        ArrowArray.wrap(importIdxFFIArrayPtr)) { (inputRoot, idxRoot, inputArray, idxArray) =>
        // import into params root
        Data.importIntoVectorSchemaRoot(batchAllocator, inputArray, inputRoot, dictionaryProvider)
        val inputRows = ColumnarHelper.rootAsBatch(inputRoot)

        Data.importIntoVectorSchemaRoot(batchAllocator, idxArray, idxRoot, dictionaryProvider)
        val fieldVectors = idxRoot.getFieldVectors.asScala
        val rowIdxVector = fieldVectors(0).asInstanceOf[IntVector]
        val inputIdxVector = fieldVectors(1).asInstanceOf[IntVector]

        for (i <- 0 until idxRoot.getRowCount) {
          val rowIdx = rowIdxVector.get(i)
          val row = rows(rowIdx)
          val input = paramsToUnsafe(inputRows.getRow(inputIdxVector.get(i)))
          rows(rowIdx) = updater(new JoinedRow(row, input)).copy()
        }
      }
    }
  }

  def merge(
      rows: ArrayBuffer[InternalRow],
      mergeRows: ArrayBuffer[InternalRow],
      importIdxFFIArrayPtr: Long): Unit = {
    Using.resource(ArrowUtils.newChildAllocator(getClass.getName)) { batchAllocator =>
      Using.resources(
        VectorSchemaRoot.create(indexSchema, batchAllocator),
        ArrowArray.wrap(importIdxFFIArrayPtr)) { (idxRoot, idxArray) =>
        Data.importIntoVectorSchemaRoot(batchAllocator, idxArray, idxRoot, dictionaryProvider)
        val fieldVectors = idxRoot.getFieldVectors.asScala
        val rowIdxVector = fieldVectors(0).asInstanceOf[IntVector]
        val mergeIdxVector = fieldVectors(1).asInstanceOf[IntVector]

        for (i <- 0 until idxRoot.getRowCount) {
          val rowIdx = rowIdxVector.get(i)
          val mergeIdx = mergeIdxVector.get(i)
          val row = rows(rowIdx)
          val mergeRow = mergeRows(mergeIdx)
          rows(rowIdx) = merger(new JoinedRow(row, mergeRow)).copy()
        }
      }
    }
  }

  def eval(
      rows: ArrayBuffer[InternalRow],
      importIdxFFIArrayPtr: Long,
      exportFFIArrayPtr: Long): Unit = {
    Using.resource(ArrowUtils.newChildAllocator(getClass.getName)) { batchAllocator =>
      Using.resources(
        VectorSchemaRoot.create(evalIndexSchema, batchAllocator),
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
