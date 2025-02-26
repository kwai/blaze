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

import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer

import org.apache.arrow.c.ArrowArray
import org.apache.arrow.c.Data
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.dictionary.DictionaryProvider
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.blaze.util.Using
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.catalyst.expressions.Nondeterministic
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate
import org.apache.spark.sql.execution.blaze.arrowio.util.ArrowUtils
import org.apache.spark.sql.execution.blaze.arrowio.util.ArrowWriter
import org.apache.spark.sql.execution.UnsafeRowSerializer
import org.apache.spark.sql.execution.blaze.arrowio.util.ArrowUtils.ROOT_ALLOCATOR
import org.apache.spark.sql.execution.blaze.columnar.ColumnarHelper
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.ByteBufferInputStream

case class SparkUDAFWrapperContext[B](serialized: ByteBuffer) extends Logging {
  private val (expr, javaParamsSchema) =
    NativeConverters.deserializeExpression[AggregateFunction, StructType]({
      val bytes = new Array[Byte](serialized.remaining())
      serialized.get(bytes)
      bytes
    })

  val inputAttributes: Seq[Attribute] = javaParamsSchema.fields.map { field =>
    AttributeReference(field.name, field.dataType, field.nullable)()
  }

  private val outputSchema = {
    val schema = StructType(Seq(StructField("", expr.dataType, expr.nullable)))
    ArrowUtils.toArrowSchema(schema)
  }

  // initialize all nondeterministic children exprs
  expr.foreach {
    case nondeterministic: Nondeterministic =>
      nondeterministic.initialize(TaskContext.get.partitionId())
    case _ =>
  }

  private val aggEvaluator = {
    val evaluator = expr match {
      case declarative: DeclarativeAggregate =>
        new DeclarativeEvaluator(declarative, inputAttributes)
      case imperative: TypedImperativeAggregate[B] =>
        new TypedImperativeEvaluator(imperative)
    }
    evaluator.asInstanceOf[AggregateEvaluator[B]]
  }

  private val dictionaryProvider: DictionaryProvider = new MapDictionaryProvider()

  private val inputSchema = ArrowUtils.toArrowSchema(javaParamsSchema)

  def initialize(numRow: Int): BufferRowsColumn[B] = {
    val rows = aggEvaluator.createEmptyColumn()
    rows.resize(numRow, aggEvaluator.initialize())
    rows
  }

  def resize(rows: BufferRowsColumn[B], len: Int): Unit = {
    rows.resize(len, aggEvaluator.initialize())
  }

  def update(
      rows: BufferRowsColumn[B],
      importBatchFFIArrayPtr: Long,
      zippedIndices: Array[Long]): Unit = {
    Using.resources(
      VectorSchemaRoot.create(inputSchema, ROOT_ALLOCATOR),
      ArrowArray.wrap(importBatchFFIArrayPtr)) { (inputRoot, inputArray) =>
      // import into params root
      Data.importIntoVectorSchemaRoot(ROOT_ALLOCATOR, inputArray, inputRoot, dictionaryProvider)
      val inputRow = ColumnarHelper.rootRowReuseable(inputRoot)

      for (zippedIdx <- zippedIndices) {
        val rowIdx = ((zippedIdx >> 32) & 0xffffffff).toInt
        val updatingRowIdx = ((zippedIdx >> 0) & 0xffffffff).toInt
        inputRow.rowId = updatingRowIdx
        rows.update(rowIdx, row => aggEvaluator.update(row, inputRow))
      }
    }
  }

  def merge(
      rows: BufferRowsColumn[B],
      mergeRows: BufferRowsColumn[B],
      zippedIndices: Array[Long]): Unit = {

    for (zippedIdx <- zippedIndices) {
      val rowIdx = ((zippedIdx >> 32) & 0xffffffff).toInt
      val mergingRowIdx = ((zippedIdx >> 0) & 0xffffffff).toInt
      rows.update(rowIdx, aggEvaluator.merge(_, mergeRows.row(mergingRowIdx)))
    }
  }

  def eval(rows: BufferRowsColumn[B], indices: Array[Int], exportFFIArrayPtr: Long): Unit = {
    Using.resources(
      VectorSchemaRoot.create(outputSchema, ROOT_ALLOCATOR),
      ArrowArray.wrap(exportFFIArrayPtr)) { (outputRoot, exportArray) =>
      // evaluate expression and write to output root
      val outputWriter = ArrowWriter.create(outputRoot)
      for (i <- indices) {
        outputWriter.write(aggEvaluator.eval(rows.row(i)))
      }
      outputWriter.finish()

      // export to output using root allocator
      Data.exportVectorSchemaRoot(ROOT_ALLOCATOR, outputRoot, dictionaryProvider, exportArray)
    }
  }

  def serializeRows(rows: BufferRowsColumn[B], indices: Array[Int]): Array[Byte] = {
    aggEvaluator.serializeRows(indices.iterator.map(i => rows.row(i)))
  }

  def deserializeRows(dataBuffer: ByteBuffer): BufferRowsColumn[B] = {
    val rows = aggEvaluator.createEmptyColumn()
    rows.append(aggEvaluator.deserializeRows(dataBuffer): _*)
    rows
  }

  def memUsed(rows: BufferRowsColumn[B]): Int = {
    rows.memUsed
  }
}

abstract class BufferRowsColumn[B] {
  protected var rows: ArrayBuffer[B] = ArrayBuffer[B]()
  protected var rowsMemUsed: Int = 0

  def length: Int = rows.length
  def memUsed: Int = rowsMemUsed

  def resize(len: Int, initializer: => B): Unit = {
    if (rows.length < len) {
      for (_ <- rows.length until len) {
        val newRow = initializer
        rowsMemUsed += getRowMemUsage(newRow)
        rows.append(newRow)
      }
    } else {
      for (i <- len until rows.length) {
        rowsMemUsed -= getRowMemUsage(rows(i))
      }
      rows.trimEnd(rows.length - len)
    }
  }

  def row(i: Int): B = rows(i)

  def append(appendedRows: B*): Unit = {
    rowsMemUsed += appendedRows.map(getRowMemUsage).sum
    rows.append(appendedRows: _*)
  }

  def update(i: Int, updater: B => B): Unit = {
    rowsMemUsed -= getRowMemUsage(rows(i))
    rows(i) = updater(rows(i))
    rowsMemUsed += getRowMemUsage(rows(i))
  }

  def getRowMemUsage(row: B): Int
}

trait AggregateEvaluator[B] extends Logging {
  def createEmptyColumn(): BufferRowsColumn[B]
  def initialize(): B
  def update(mutableAggBuffer: B, row: InternalRow): B
  def merge(row1: B, row2: B): B
  def eval(row: B): InternalRow
  def serializeRows(rows: Iterator[B]): Array[Byte]
  def deserializeRows(dataBuffer: ByteBuffer): Seq[B]
}

class DeclarativeEvaluator(agg: DeclarativeAggregate, inputAttributes: Seq[Attribute])
    extends AggregateEvaluator[UnsafeRow] {

  private val initializer = UnsafeProjection.create(agg.initialValues)
  private val initializedRow = initializer(InternalRow.empty)

  private val updater =
    UnsafeProjection.create(agg.updateExpressions, agg.aggBufferAttributes ++ inputAttributes)

  private val merger = UnsafeProjection.create(
    agg.mergeExpressions,
    agg.aggBufferAttributes ++ agg.inputAggBufferAttributes)

  private val evaluator =
    UnsafeProjection.create(agg.evaluateExpression :: Nil, agg.aggBufferAttributes)

  private val joiner = new JoinedRow

  override def createEmptyColumn(): BufferRowsColumn[UnsafeRow] = {
    new BufferRowsColumn[UnsafeRow]() {
      override def getRowMemUsage(row: UnsafeRow): Int = {
        row.getSizeInBytes
      }
    }
  }

  override def initialize(): UnsafeRow = {
    initializedRow.copy()
  }

  override def update(mutableAggBuffer: UnsafeRow, row: InternalRow): UnsafeRow = {
    updater(joiner(mutableAggBuffer, row)).copy()
  }

  override def merge(row1: UnsafeRow, row2: UnsafeRow): UnsafeRow = {
    merger(joiner(row1, row2)).copy()
  }

  override def eval(row: UnsafeRow): UnsafeRow = {
    evaluator(row)
  }

  override def serializeRows(rows: Iterator[UnsafeRow]): Array[Byte] = {
    val numFields = agg.aggBufferSchema.length
    val outputDataStream = new ByteArrayOutputStream()
    val serializer = new UnsafeRowSerializer(numFields).newInstance()

    Using(serializer.serializeStream(outputDataStream)) { ser =>
      for (row <- rows) {
        ser.writeValue(row)
      }
    }
    outputDataStream.toByteArray
  }

  override def deserializeRows(dataBuffer: ByteBuffer): Seq[UnsafeRow] = {
    val numFields = agg.aggBufferSchema.length
    val deserializer = new UnsafeRowSerializer(numFields).newInstance()
    val inputDataStream = new ByteBufferInputStream(dataBuffer)
    val rows = new ArrayBuffer[UnsafeRow]()

    Using.resource(deserializer.deserializeStream(inputDataStream)) { deser =>
      for (row <- deser.asKeyValueIterator.map(_._2.asInstanceOf[UnsafeRow].copy())) {
        rows.append(row)
      }
    }
    rows
  }
}

class TypedImperativeEvaluator[B](agg: TypedImperativeAggregate[B])
    extends AggregateEvaluator[B] {
  private val evalRow = InternalRow(0)

  private val memUse = BlazeConf.SUGGESTED_UDAF_ROW_MEM_USAGE.intConf()
  override def createEmptyColumn(): BufferRowsColumn[B] = {
    new BufferRowsColumn[B]() {
      override def getRowMemUsage(row: B): Int = {
        memUse // estimated size of object
      }

      override def update(i: Int, updater: B => B): Unit = {
        rows(i) = updater(rows(i))
      }
    }
  }

  override def initialize(): B = {
    agg.createAggregationBuffer()
  }

  override def update(buffer: B, row: InternalRow): B = {
    agg.update(buffer, row)
  }

  override def merge(row1: B, row2: B): B = {
    agg.merge(row1, row2)
  }

  override def eval(row: B): InternalRow = {
    evalRow.update(0, agg.eval(row))
    evalRow
  }

  override def serializeRows(rows: Iterator[B]): Array[Byte] = {
    val outputStream = new ByteArrayOutputStream()
    val dataOut = new DataOutputStream(outputStream)
    for (row <- rows) {
      val byteBuffer = agg.serialize(row)
      dataOut.writeInt(byteBuffer.length)
      dataOut.write(byteBuffer)
    }
    outputStream.toByteArray
  }

  override def deserializeRows(dataBuffer: ByteBuffer): Seq[B] = {
    val rows = ArrayBuffer[B]()
    while (dataBuffer.hasRemaining) {
      val length = dataBuffer.getInt()
      val byteBuffer = new Array[Byte](length)
      dataBuffer.get(byteBuffer)
      val row = agg.deserialize(byteBuffer)
      rows.append(row)
    }
    rows
  }
}
