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

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.nio.ByteBuffer
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import org.apache.arrow.c.ArrowArray
import org.apache.arrow.c.Data
import org.apache.arrow.vector.IntVector
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
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.blaze.arrowio.ColumnarHelper
import org.apache.spark.sql.execution.blaze.arrowio.util.ArrowUtils
import org.apache.spark.sql.execution.blaze.arrowio.util.ArrowWriter
import org.apache.spark.sql.execution.UnsafeRowSerializer
import org.apache.spark.sql.types.{BinaryType, IntegerType, ObjectType, StructField, StructType}
import org.apache.spark.util.ByteBufferInputStream

case class SparkUDAFWrapperContext(serialized: ByteBuffer) extends Logging {
  import org.apache.spark.sql.blaze.SparkUDAFWrapperContext._

  private val (expr, List(javaParamsSchema, javaBufferSchema)) =
    NativeConverters.deserializeExpression[AggregateFunction, List[StructType]]({
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

  def initialize(numRow: Int): ArrayBuffer[InternalRow] = {
    val rows = ArrayBuffer[InternalRow]()
    resize(rows, numRow)
    rows
  }

  def resize(rows: ArrayBuffer[InternalRow], len: Int): Unit = {
    if (rows.length < len) {
      rows.append(Range(rows.length, len).map(_ => aggEvaluator.initialize()): _*)
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
        VectorSchemaRoot.create(indexTupleSchema, batchAllocator),
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
      rows: ArrayBuffer[InternalRow],
      mergeRows: ArrayBuffer[InternalRow],
      importIdxFFIArrayPtr: Long): Unit = {
    Using.resource(ArrowUtils.newChildAllocator(getClass.getName)) { batchAllocator =>
      Using.resources(
        VectorSchemaRoot.create(indexTupleSchema, batchAllocator),
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
      rows: ArrayBuffer[InternalRow],
      importIdxFFIArrayPtr: Long,
      exportFFIArrayPtr: Long): Unit = {
    Using.resource(ArrowUtils.newChildAllocator(getClass.getName)) { batchAllocator =>
      Using.resources(
        VectorSchemaRoot.create(indexSchema, batchAllocator),
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

  def serializeRows(
    rows: ArrayBuffer[InternalRow],
    importFFIArrayPtr: Long,
    exportFFIArrayPtr: Long): Unit = {

    Using.resource(ArrowUtils.newChildAllocator(getClass.getName)) { batchAllocator =>
      Using.resources(
        VectorSchemaRoot.create(serializedRowSchema, batchAllocator),
        VectorSchemaRoot.create(indexSchema, batchAllocator),
      ) { (exportDataRoot, importIdxRoot) =>

        Using.resources(
          ArrowArray.wrap(importFFIArrayPtr),
          ArrowArray.wrap(exportFFIArrayPtr)) { (importArray, exportArray) =>

          // import into params root
          Data.importIntoVectorSchemaRoot(
            batchAllocator,
            importArray,
            importIdxRoot,
            dictionaryProvider)

          // write serialized row into sequential raw bytes
          val importIdxArray = importIdxRoot.getFieldVectors.get(0).asInstanceOf[IntVector]
          val rowsIter = (0 until importIdxRoot.getRowCount).map(i => rows(importIdxArray.get(i)))
          val serializedBytes = aggEvaluator.serializeRows(rowsIter)

          // export serialized data as a single row batch using root allocator
          val outputWriter = ArrowWriter.create(exportDataRoot)
          outputWriter.write(InternalRow(serializedBytes))
          outputWriter.finish()
          Data.exportVectorSchemaRoot(
            ArrowUtils.rootAllocator,
            exportDataRoot,
            dictionaryProvider,
            exportArray)
        }
      }
    }
  }

  def deserializeRows(dataBuffer: ByteBuffer): ArrayBuffer[InternalRow] = {
    aggEvaluator.deserializeRows(dataBuffer)
  }
}

object SparkUDAFWrapperContext {
  private val indexTupleSchema = {
    val schema = StructType(Seq(StructField("", IntegerType), StructField("", IntegerType)))
    ArrowUtils.toArrowSchema(schema)
  }

  private val indexSchema = {
    val schema = StructType(Seq(StructField("", IntegerType, nullable = false)))
    ArrowUtils.toArrowSchema(schema)
  }

  private val serializedRowSchema = {
    val schema = StructType(Seq(StructField("", BinaryType, nullable = false)))
    ArrowUtils.toArrowSchema(schema)
  }
}

trait AggregateEvaluator extends Logging {
  def initialize(): InternalRow
  def update(mutableAggBuffer: InternalRow, row: InternalRow): InternalRow
  def merge(row1: InternalRow, row2: InternalRow): InternalRow
  def eval(row: InternalRow): InternalRow
  def serializeRows(rows: Seq[InternalRow]): Array[Byte]
  def deserializeRows(dataBuffer: ByteBuffer): ArrayBuffer[InternalRow]
}

class DeclarativeEvaluator(agg: DeclarativeAggregate, inputAttributes: Seq[Attribute])
    extends AggregateEvaluator {

  private val initializer = UnsafeProjection.create(agg.initialValues)

  private val updater =
    UnsafeProjection.create(agg.updateExpressions, agg.aggBufferAttributes ++ inputAttributes)

  private val merger = UnsafeProjection.create(
    agg.mergeExpressions,
    agg.aggBufferAttributes ++ agg.inputAggBufferAttributes)

  private val evaluator =
    UnsafeProjection.create(agg.evaluateExpression :: Nil, agg.aggBufferAttributes)


  override def initialize(): InternalRow = {
    initializer.apply(InternalRow.empty)
  }

  override def update(mutableAggBuffer: InternalRow, row: InternalRow): InternalRow = {
    updater(new JoinedRow(mutableAggBuffer, row)).copy()
  }

  override def merge(row1: InternalRow, row2: InternalRow): InternalRow = {
    merger(new JoinedRow(row1, row2)).copy()
  }

  override def eval(row: InternalRow): InternalRow = {
    evaluator(row)
  }

  override def serializeRows(rows: Seq[InternalRow]): Array[Byte] = {
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

  override def deserializeRows(dataBuffer: ByteBuffer): ArrayBuffer[InternalRow] = {
    val numFields = agg.aggBufferSchema.length
    val deserializer = new UnsafeRowSerializer(numFields).newInstance()
    val inputDataStream = new ByteBufferInputStream(dataBuffer)
    val rows = new ArrayBuffer[InternalRow]()

    Using.resource(deserializer.deserializeStream(inputDataStream)) { deser =>
      for (row <- deser.asKeyValueIterator.map(_._2.asInstanceOf[UnsafeRow].copy())) {
        rows.append(row)
      }
    }
    rows
  }
}

class TypedImperativeEvaluator[T](agg: TypedImperativeAggregate[T], inputAttributes: Seq[Attribute])
    extends AggregateEvaluator {

  private val bufferSchema =  agg.aggBufferAttributes.map(_.dataType)
  private val anyObjectType = ObjectType(classOf[AnyRef])

  private def getBufferObject(buffer: InternalRow): T = {
    buffer.get(0, anyObjectType).asInstanceOf[T]
  }
  override def initialize(): InternalRow = {
    val row = InternalRow(bufferSchema)
    agg.initialize(row)
    row
  }

  override def update(buffer: InternalRow, row: InternalRow): InternalRow = {
    agg.update(buffer, row)
    buffer
  }

  override def merge(row1: InternalRow, row2: InternalRow): InternalRow = {
    val Object1 = getBufferObject(row1)
    val Object2 = getBufferObject(row2)
    row1.update(0, agg.merge(Object1, Object2))
    row1
  }

  override def eval(row: InternalRow): InternalRow = {
    InternalRow(agg.eval(row))
  }

  override def serializeRows(rows: Seq[InternalRow]): Array[Byte] = {
    val outputStream = new ByteArrayOutputStream()
    val dataOut = new DataOutputStream(outputStream)
    for (row <- rows) {
      val byteBuffer = agg.serialize(row.get(0, anyObjectType).asInstanceOf[T])
      dataOut.writeInt(byteBuffer.length)
      outputStream.write(byteBuffer)
    }
    outputStream.toByteArray
  }

  override def deserializeRows(dataBuffer: ByteBuffer): ArrayBuffer[InternalRow] = {
    val rows = ArrayBuffer[InternalRow]()
    while (dataBuffer.hasRemaining) {
      val length = dataBuffer.getInt()
      val byteBuffer = new Array[Byte](length)
      dataBuffer.get(byteBuffer)
      val row = InternalRow(agg.deserialize(byteBuffer))
      rows.append(row)
    }
    rows
  }
}
