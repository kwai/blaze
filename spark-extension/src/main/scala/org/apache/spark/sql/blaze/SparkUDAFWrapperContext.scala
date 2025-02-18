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
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, DeclarativeAggregate, ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, BindReferences, GenericInternalRow, JoinedRow, Nondeterministic, SpecificInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.blaze.arrowio.ColumnarHelper
import org.apache.spark.sql.execution.blaze.arrowio.util.{ArrowUtils, ArrowWriter}
import org.apache.spark.sql.types.{BinaryType, DataType, IntegerType, ObjectType, StructField, StructType}
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter

import java.nio.ByteBuffer
import scala.collection.mutable.ArrayBuffer

case class SparkUDAFWrapperContext(serialized: ByteBuffer) extends Logging {
  private val (expr, javaParamsSchema) =
    NativeConverters.deserializeExpression[AggregateFunction]({
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

  private val aggEvaluator = expr match {
    case declarative: DeclarativeAggregate =>
      logInfo(s"init DeclarativeEvaluator")
      new DeclarativeEvaluator(declarative, inputAttributes)
    case imperative: TypedImperativeAggregate[_] =>
      logInfo(s"init TypedImperativeEvaluator")
      new TypedImperativeEvaluator(imperative, inputAttributes)
  }

  private val dictionaryProvider: DictionaryProvider = new MapDictionaryProvider()

  private val inputSchema = ArrowUtils.toArrowSchema(javaParamsSchema)
  private val paramsToUnsafe = {
    val toUnsafe = UnsafeProjection.create(javaParamsSchema)
    toUnsafe.initialize(Option(TaskContext.get()).map(_.partitionId()).getOrElse(0))
    toUnsafe
  }

  private val outputSchema = {
    val schema = StructType(Seq(StructField("", expr.dataType, expr.nullable)))
    ArrowUtils.toArrowSchema(schema)
  }

  private val indexSchema = {
    val schema = StructType(Seq(StructField("", IntegerType), StructField("", IntegerType)))
    ArrowUtils.toArrowSchema(schema)
  }

  private val evalIndexSchema = {
    val schema = StructType(Seq(StructField("", IntegerType)))
    ArrowUtils.toArrowSchema(schema)
  }


  def initialize(numRow: Int): ArrayBuffer[UnsafeRow] = {
    val rows = ArrayBuffer[UnsafeRow]()
    resize(rows, numRow)
    rows
  }

  def resize(rows: ArrayBuffer[UnsafeRow], len: Int): Unit = {
    if (rows.length < len) {
      rows.append(Range(rows.length, len).map(_ => aggEvaluator.initialize()): _*)
    } else {
      rows.trimEnd(rows.length - len)
    }
  }

  def update(
      rows: ArrayBuffer[UnsafeRow],
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
          rows(rowIdx) = aggEvaluator.update(row, input)
        }
      }
    }
  }

  def merge(
      rows: ArrayBuffer[UnsafeRow],
      mergeRows: ArrayBuffer[UnsafeRow],
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
          rows(rowIdx) = aggEvaluator.merge(row, mergeRow)
        }
      }
    }
  }

  def eval(
      rows: ArrayBuffer[UnsafeRow],
      importIdxFFIArrayPtr: Long,
      exportFFIArrayPtr: Long): Unit = {
    Using.resource(ArrowUtils.newChildAllocator(getClass.getName)) { batchAllocator =>
      Using.resources(
        VectorSchemaRoot.create(evalIndexSchema, batchAllocator),
        VectorSchemaRoot.create(outputSchema, batchAllocator),
        ArrowArray.wrap(importIdxFFIArrayPtr),
        ArrowArray.wrap(exportFFIArrayPtr)) { (idxRoot, outputRoot, idxArray, exportArray) =>
        Data.importIntoVectorSchemaRoot(batchAllocator, idxArray, idxRoot, dictionaryProvider)
        val fieldVectors = idxRoot.getFieldVectors.asScala
        val rowIdxVector = fieldVectors.head.asInstanceOf[IntVector]

        // evaluate expression and write to output root
        val outputWriter = ArrowWriter.create(outputRoot)
        for (i <- 0 until idxRoot.getRowCount) {
          val row = rows(rowIdxVector.get(i))
          outputWriter.write(aggEvaluator.eval(row))
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

trait AggregateEvaluator extends Logging {
  def initialize(): UnsafeRow
  def update(mutableAggBuffer: UnsafeRow, row: UnsafeRow): UnsafeRow
  def merge(row1: UnsafeRow, row2: UnsafeRow): UnsafeRow
  def eval(row: UnsafeRow): InternalRow
}

class DeclarativeEvaluator(agg: DeclarativeAggregate, inputAttributes: Seq[Attribute])
    extends AggregateEvaluator {

  private lazy val initializer = UnsafeProjection.create(agg.initialValues)

  private lazy val updater =
    UnsafeProjection.create(agg.updateExpressions, agg.aggBufferAttributes ++ inputAttributes)

  private lazy val merger = UnsafeProjection.create(
    agg.mergeExpressions,
    agg.aggBufferAttributes ++ agg.inputAggBufferAttributes)

  private lazy val evaluator =
    UnsafeProjection.create(agg.evaluateExpression :: Nil, agg.aggBufferAttributes)

  new MapDictionaryProvider()

  override def initialize(): UnsafeRow = {
    initializer.apply(InternalRow.empty)
  }

  override def update(mutableAggBuffer: UnsafeRow, row: UnsafeRow): UnsafeRow = {
    updater(new JoinedRow(mutableAggBuffer, row)).copy()
  }

  override def merge(row1: UnsafeRow, row2: UnsafeRow): UnsafeRow = {
    merger(new JoinedRow(row1, row2)).copy()
  }

  override def eval(row: UnsafeRow): InternalRow = {
    evaluator(row)
  }
}

class TypedImperativeEvaluator[T](agg: TypedImperativeAggregate[T], inputAttributes: Seq[Attribute])
    extends AggregateEvaluator {

  private val bufferSchema =  agg.aggBufferAttributes.map(_.dataType)

  private def getBufferObject(bufferRow: UnsafeRow): T = {
    agg.deserialize(bufferRow.getBytes)
  }


  override def initialize(): UnsafeRow = {
//    val byteBuffer = agg.serialize(agg.createAggregationBuffer())
//    val unsafeRow = new UnsafeRow(bufferSchema.length)
//    unsafeRow.pointTo(byteBuffer, byteBuffer.length)
//    logInfo(s"bufferSchema: $bufferSchema")
    val buffer = new  SpecificInternalRow(bufferSchema)
    agg.initialize(buffer)
//    logInfo(s"buffer $buffer")
    val writer = new UnsafeRowWriter(1)
    writer.write(0, buffer.getBinary(0))
    val unsafeRow = writer.getRow
//    logInfo(s"init unsaferow $unsafeRow")
    unsafeRow
  }

  override def update(buffer: UnsafeRow, row: UnsafeRow): UnsafeRow = {
    val bufferObject = agg.update(getBufferObject(buffer), row)
    val byteBuffer = agg.serialize(bufferObject).clone()
    buffer.pointTo(byteBuffer, byteBuffer.length)
    buffer
  }

  override def merge(row1: UnsafeRow, row2: UnsafeRow): UnsafeRow = {
    val bufferObject = agg.merge(getBufferObject(row1), getBufferObject(row2))
    val byteBuffer = agg.serialize(bufferObject).clone()
    row1.pointTo(byteBuffer, byteBuffer.length)
    row1
  }

  override def eval(row: UnsafeRow): InternalRow = {
    InternalRow(agg.eval(getBufferObject(row)))
  }
}
