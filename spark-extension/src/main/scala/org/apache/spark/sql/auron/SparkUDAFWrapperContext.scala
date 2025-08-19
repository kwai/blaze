/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.auron

import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.EOFException
import java.io.InputStream
import java.io.OutputStream
import java.nio.ByteBuffer

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.arrow.c.ArrowArray
import org.apache.arrow.c.Data
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.dictionary.DictionaryProvider
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.auron.memory.OnHeapSpillManager
import org.apache.spark.sql.auron.util.Using
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
import org.apache.spark.sql.execution.auron.arrowio.util.ArrowUtils
import org.apache.spark.sql.execution.auron.arrowio.util.ArrowWriter
import org.apache.spark.sql.execution.UnsafeRowSerializer
import org.apache.spark.sql.execution.auron.arrowio.util.ArrowUtils.ROOT_ALLOCATOR
import org.apache.spark.sql.execution.auron.columnar.ColumnarHelper
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.ByteBufferInputStream
import org.apache.spark.SparkEnv
import org.apache.spark.io.SnappyCompressionCodec
import org.apache.spark.memory.MemoryConsumer
import org.apache.spark.memory.MemoryMode
import org.apache.spark.util.Utils

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
      nondeterministic.initialize(TaskContext.get match {
        case tc: TaskContext => tc.partitionId()
        case null => 0
      })
    case _ =>
  }

  private val inputProjection = UnsafeProjection.create(javaParamsSchema)

  private val aggEvaluator = new ThreadLocal[AggregateEvaluator[B, BufferRowsColumn[B]]] {
    override def initialValue: AggregateEvaluator[B, BufferRowsColumn[B]] = {
      val evaluator = expr match {
        case declarative: DeclarativeAggregate =>
          new DeclarativeEvaluator(declarative, inputAttributes)
        case imperative: TypedImperativeAggregate[B] =>
          new TypedImperativeEvaluator(imperative)
      }
      evaluator.asInstanceOf[AggregateEvaluator[B, BufferRowsColumn[B]]]
    }
  }

  private val dictionaryProvider: DictionaryProvider = new MapDictionaryProvider()

  private val inputSchema = ArrowUtils.toArrowSchema(javaParamsSchema)

  def initialize(numRow: Int): BufferRowsColumn[B] = {
    val rows = aggEvaluator.get.createEmptyColumn()
    rows.resize(numRow)
    rows
  }

  def resize(rows: BufferRowsColumn[B], len: Int): Unit = {
    rows.resize(len)
  }

  def numRecords(rows: BufferRowsColumn[B]): Int = {
    rows.length
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
      val inputRow = ColumnarHelper.rootRowReusable(inputRoot)

      for (zippedIdx <- zippedIndices) {
        val rowIdx = ((zippedIdx >> 32) & 0xffffffff).toInt
        val updatingRowIdx = ((zippedIdx >> 0) & 0xffffffff).toInt
        inputRow.rowId = updatingRowIdx
        rows.updateRow(rowIdx, inputProjection(inputRow).copy())
      }
    }
  }

  def merge(
      rows: BufferRowsColumn[B],
      mergeRows: BufferRowsColumn[B],
      zippedIndices: Array[Long]): Unit = {

    for (zippedIdx <- zippedIndices) {
      val rowIdx = ((zippedIdx >> 32) & 0xffffffff).toInt
      val mergeIdx = ((zippedIdx >> 0) & 0xffffffff).toInt
      rows.mergeRow(rowIdx, mergeRows, mergeIdx)
    }
  }

  def eval(rows: BufferRowsColumn[B], indices: Array[Int], exportFFIArrayPtr: Long): Unit = {
    Using.resources(
      VectorSchemaRoot.create(outputSchema, ROOT_ALLOCATOR),
      ArrowArray.wrap(exportFFIArrayPtr)) { (outputRoot, exportArray) =>
      // evaluate expression and write to output root
      val outputWriter = ArrowWriter.create(outputRoot)
      for (i <- indices) {
        outputWriter.write(rows.evalRow(i))
      }
      outputWriter.finish()

      // export to output using root allocator
      Data.exportVectorSchemaRoot(ROOT_ALLOCATOR, outputRoot, dictionaryProvider, exportArray)
    }
  }

  def serializeRows(rows: BufferRowsColumn[B], indices: Array[Int]): Array[Byte] = {
    aggEvaluator.get.serializeRows(rows, indices.iterator)
  }

  def deserializeRows(dataBuffer: ByteBuffer): BufferRowsColumn[B] = {
    aggEvaluator.get.deserializeRows(dataBuffer)
  }

  def spill(
      memTracker: SparkUDAFMemTracker,
      rows: BufferRowsColumn[B],
      indices: Array[Int],
      spillIdx: Long): Int = {
    aggEvaluator.get.spill(memTracker, rows, indices.iterator, spillIdx)
  }

  def unspill(
      memTracker: SparkUDAFMemTracker,
      spillId: Int,
      spillIdx: Long): BufferRowsColumn[B] = {
    aggEvaluator.get.unspill(memTracker, spillId, spillIdx)
  }
}

trait BufferRowsColumn[B] {
  def length: Int
  def memUsed: Int
  def resize(numRows: Int): Unit
  def updateRow(i: Int, inputRow: InternalRow): Unit
  def mergeRow(i: Int, mergeRows: BufferRowsColumn[B], mergeIdx: Int): Unit
  def evalRow(i: Int): InternalRow
}

trait AggregateEvaluator[B, R <: BufferRowsColumn[B]] extends Logging {
  private lazy val spillCodec = new SnappyCompressionCodec(SparkEnv.get.conf)

  def createEmptyColumn(): R

  def serializeRows(
      rows: R,
      indices: Iterator[Int],
      streamWrapper: OutputStream => OutputStream = { s => s }): Array[Byte]

  def deserializeRows(
      dataBuffer: ByteBuffer,
      streamWrapper: InputStream => InputStream = { s => s }): R

  def spill(
      memTracker: SparkUDAFMemTracker,
      rows: R,
      indices: Iterator[Int],
      spillIdx: Long): Int = {
    val hsm = OnHeapSpillManager.current
    val spillId = memTracker.getSpill(spillIdx)
    val byteBuffer =
      ByteBuffer.wrap(serializeRows(rows, indices, spillCodec.compressedOutputStream))
    val spillBlockSize = byteBuffer.limit()
    hsm.writeSpill(spillId, byteBuffer)
    spillBlockSize
  }

  def unspill(
      memTracker: SparkUDAFMemTracker,
      spillBlockSize: Int,
      spillIdx: Long): BufferRowsColumn[B] = {
    val hsm = OnHeapSpillManager.current
    val spillId = memTracker.getSpill(spillIdx)
    val byteBuffer = ByteBuffer.allocate(spillBlockSize)
    val readSize = hsm.readSpill(spillId, byteBuffer).toLong
    assert(readSize == spillBlockSize)
    byteBuffer.flip()
    deserializeRows(byteBuffer, spillCodec.compressedInputStream)
  }
}

class DeclarativeEvaluator(val agg: DeclarativeAggregate, inputAttributes: Seq[Attribute])
    extends AggregateEvaluator[UnsafeRow, DeclarativeAggRowsColumn] {

  val initializedRow: UnsafeRow = {
    val initializer = UnsafeProjection.create(agg.initialValues)
    initializer(InternalRow.empty)
  }
  val releasedRow: UnsafeRow = null

  val updater: UnsafeProjection =
    UnsafeProjection.create(agg.updateExpressions, agg.aggBufferAttributes ++ inputAttributes)

  val merger: UnsafeProjection = UnsafeProjection.create(
    agg.mergeExpressions,
    agg.aggBufferAttributes ++ agg.inputAggBufferAttributes)

  val evaluator: UnsafeProjection =
    UnsafeProjection.create(agg.evaluateExpression :: Nil, agg.aggBufferAttributes)

  val joiner = new JoinedRow

  override def createEmptyColumn(): DeclarativeAggRowsColumn = {
    DeclarativeAggRowsColumn(this, ArrayBuffer())
  }

  override def serializeRows(
      rows: DeclarativeAggRowsColumn,
      indices: Iterator[Int],
      streamWrapper: OutputStream => OutputStream): Array[Byte] = {

    val numFields = agg.aggBufferSchema.length
    val outputDataStream = new ByteArrayOutputStream()
    val wrappedStream = streamWrapper(outputDataStream)
    val serializer = new UnsafeRowSerializer(numFields).newInstance()

    Using(serializer.serializeStream(wrappedStream)) { ser =>
      for (i <- indices) {
        ser.writeValue(rows.rows(i))
        rows.rows(i) = releasedRow
      }
    }
    wrappedStream.close()
    outputDataStream.toByteArray
  }

  override def deserializeRows(
      dataBuffer: ByteBuffer,
      streamWrapper: InputStream => InputStream): DeclarativeAggRowsColumn = {
    val numFields = agg.aggBufferSchema.length
    val deserializer = new UnsafeRowSerializer(numFields).newInstance()
    val inputDataStream = new ByteBufferInputStream(dataBuffer)
    val wrappedStream = streamWrapper(inputDataStream)
    val rows = new ArrayBuffer[UnsafeRow]()

    Using.resource(deserializer.deserializeStream(wrappedStream)) { deser =>
      for (row <- deser.asKeyValueIterator.map(_._2.asInstanceOf[UnsafeRow].copy())) {
        rows.append(row)
      }
    }
    wrappedStream.close()
    DeclarativeAggRowsColumn(this, rows)
  }
}

case class DeclarativeAggRowsColumn(
    evaluator: DeclarativeEvaluator,
    rows: ArrayBuffer[UnsafeRow],
    var rowsMemUsed: Int = -1)
    extends BufferRowsColumn[UnsafeRow] {

  if (rowsMemUsed < 0) {
    rowsMemUsed = rows.foldLeft(0)(_ + _.getSizeInBytes)
  }

  override def length: Int = rows.length
  override def memUsed: Int = rowsMemUsed

  override def resize(len: Int): Unit = {
    rows.appendAll((rows.length until len).map(_ => {
      val newRow = evaluator.initializedRow.copy()
      rowsMemUsed += newRow.getSizeInBytes
      newRow
    }))
    rowsMemUsed -= rows
      .slice(len, rows.length)
      .filter(_ != null)
      .foldLeft(0)(_ + _.getSizeInBytes)
    rows.trimEnd(rows.length - len)
  }

  override def updateRow(i: Int, inputRow: InternalRow): Unit = {
    if (i == rows.length) {
      val newRow = evaluator.updater(evaluator.joiner(evaluator.initializedRow.copy(), inputRow))
      rowsMemUsed += newRow.getSizeInBytes
      rows.append(newRow)
    } else {
      rowsMemUsed -= rows(i).getSizeInBytes
      rows(i) = evaluator.updater(evaluator.joiner(rows(i), inputRow))
      rowsMemUsed += rows(i).getSizeInBytes
    }
  }

  override def mergeRow(i: Int, mergeRows: BufferRowsColumn[UnsafeRow], mergeIdx: Int): Unit = {
    mergeRows match {
      case mergeRows: DeclarativeAggRowsColumn =>
        if (i == rows.length) {
          val newRow = mergeRows.rows(mergeIdx)
          rowsMemUsed += newRow.getSizeInBytes
          rows.append(newRow)
        } else {
          rowsMemUsed -= rows(i).getSizeInBytes
          rows(i) = evaluator.merger(evaluator.joiner(rows(i), mergeRows.rows(mergeIdx)))
          rowsMemUsed += rows(i).getSizeInBytes
        }
        mergeRows.rows(mergeIdx) = evaluator.releasedRow
    }
  }

  override def evalRow(i: Int): InternalRow = {
    val evaluated = evaluator.evaluator(rows(i))
    rows(i) = evaluator.releasedRow
    evaluated
  }
}

class TypedImperativeEvaluator[B](val agg: TypedImperativeAggregate[B])
    extends AggregateEvaluator[B, TypedImperativeAggRowsColumn[B]] {

  val evalRow: InternalRow = InternalRow(0)
  val releasedRow: RowType = null
  var estimatedRowSize: Option[Int] = None

  override def createEmptyColumn(): TypedImperativeAggRowsColumn[B] = {
    new TypedImperativeAggRowsColumn[B](this, ArrayBuffer())
  }

  override def serializeRows(
      rows: TypedImperativeAggRowsColumn[B],
      indices: Iterator[Int],
      streamWrapper: OutputStream => OutputStream): Array[Byte] = {

    val outputStream = new ByteArrayOutputStream()
    val wrappedStream = streamWrapper(outputStream)
    val dataOut = new DataOutputStream(wrappedStream)

    for (i <- indices) {
      val bytes = rows.serializedRow(i)
      dataOut.writeInt(bytes.length)
      dataOut.write(bytes)
      rows.rows(i) = releasedRow
    }
    dataOut.close()
    outputStream.toByteArray
  }

  override def deserializeRows(
      dataBuffer: ByteBuffer,
      streamWrapper: InputStream => InputStream): TypedImperativeAggRowsColumn[B] = {
    val rows = ArrayBuffer[RowType]()
    val inputStream = new ByteBufferInputStream(dataBuffer)
    val wrappedStream = streamWrapper(inputStream)
    val dataIn = new DataInputStream(wrappedStream)
    var finished = false

    while (!finished) {
      var length = -1
      try {
        length = dataIn.readInt()
      } catch {
        case _: EOFException =>
          finished = true
      }

      if (!finished) {
        val bytes = new Array[Byte](length)
        dataIn.read(bytes)
        rows.append(SerializedRowType(bytes))
      }
    }
    dataIn.close()
    TypedImperativeAggRowsColumn(this, rows)
  }
}

trait RowType
case class SerializedRowType(bytes: Array[Byte]) extends RowType
case class DeserializedRowType[B](row: B) extends RowType

case class TypedImperativeAggRowsColumn[B](
    evaluator: TypedImperativeEvaluator[B],
    rows: ArrayBuffer[RowType])
    extends BufferRowsColumn[B] {

  override def length: Int = rows.length
  override def memUsed: Int = {
    evaluator.estimatedRowSize match {
      case Some(estimRowSize) => rows.length * estimRowSize
      case None =>
        val N = 1000 // estimate row size using first N rows
        val estimRowSize =
          if (rows.length >= N) {
            val totalSize = (0 until N)
              .foldLeft(0)((total, i) => {
                total + (serializedRow(i) match {
                  case row if row != null => row.length
                  case null => 0
                })
              })
            val estimRowSize = totalSize / N * 4 + 16
            evaluator.estimatedRowSize = Some(estimRowSize)
            estimRowSize
          } else {
            AuronConf.UDAF_FALLBACK_ESTIM_ROW_SIZE.intConf()
          }
        rows.length * estimRowSize
    }
  }

  def deserializedRow(i: Int): B = {
    rows(i) match {
      case SerializedRowType(bytes) => evaluator.agg.deserialize(bytes)
      case DeserializedRowType(row) => row.asInstanceOf[B]
    }
  }

  def serializedRow(i: Int): Array[Byte] = {
    rows(i) match {
      case SerializedRowType(bytes) => bytes
      case DeserializedRowType(row) => evaluator.agg.serialize(row.asInstanceOf[B])
    }
  }

  override def resize(len: Int): Unit = {
    rows.appendAll((rows.length until len).map { _ =>
      DeserializedRowType(evaluator.agg.createAggregationBuffer())
    })
    rows.trimEnd(rows.length - len)
  }

  override def updateRow(i: Int, inputRow: InternalRow): Unit = {
    if (i < rows.length) {
      val updated = evaluator.agg.update(deserializedRow(i), inputRow)
      rows(i) = DeserializedRowType(updated)
    } else {
      val inserted = evaluator.agg.update(evaluator.agg.createAggregationBuffer(), inputRow)
      rows.append(DeserializedRowType(inserted))
    }
  }

  override def mergeRow(i: Int, mergeRows: BufferRowsColumn[B], mergeIdx: Int): Unit = {
    mergeRows match {
      case mergeRows @ TypedImperativeAggRowsColumn(_, _) =>
        if (i < rows.length) {
          val a = deserializedRow(i)
          val b = mergeRows.deserializedRow(mergeIdx)
          val merged = evaluator.agg.merge(a, b)
          rows(i) = DeserializedRowType(merged)
        } else {
          rows.append(mergeRows.rows(mergeIdx))
        }
        mergeRows.rows(mergeIdx) = evaluator.releasedRow
    }
  }

  override def evalRow(i: Int): InternalRow = {
    val finalValue = evaluator.agg.eval(deserializedRow(i))
    rows(i) = evaluator.releasedRow
    evaluator.evalRow.update(0, finalValue)
    evaluator.evalRow
  }
}

class SparkUDAFMemTracker
    extends MemoryConsumer(
      TaskContext.get.taskMemoryManager,
      TaskContext.get.taskMemoryManager.pageSizeBytes(),
      MemoryMode.ON_HEAP)
    with Logging {

  private val columns = mutable.ArrayBuffer[BufferRowsColumn[_]]()
  private val spills = mutable.Map[Long, Int]()
  private var shouldSpill = false

  def addColumn(column: BufferRowsColumn[_]): Unit = {
    columns.append(column)
  }

  def reset(): Unit = {
    columns.clear()
    this.shouldSpill = false
    this.freeMemory(this.getUsed)
  }

  def getSpill(spillIdx: Long): Int = {
    this.spills.getOrElseUpdate(
      spillIdx, {
        OnHeapSpillManager.current.newSpill()
      })
  }

  // return true if the memory usage is successfully updated, otherwise false
  // we should spill when returning false
  def updateUsed(): Boolean = {
    if (!shouldSpill) {
      val currentUsed = columns.foldLeft(0)(_ + _.memUsed)
      val increased = currentUsed - this.getUsed
      if (increased > 0) {
        val acquired = this.acquireMemory(increased)
        if (acquired < increased) {
          shouldSpill = true
        }
      } else {
        this.freeMemory(-increased)
      }
    }
    !shouldSpill
  }

  override def spill(size: Long, memoryConsumer: MemoryConsumer): Long = {
    if (memoryConsumer == this) {
      val numRows = columns.headOption.map(_.length).getOrElse(0)
      val numCols = columns.length
      val memUsed = Utils.bytesToString(this.getUsed)
      logWarning(
        s"$this triggered spilling, numRows=$numRows, numCols=$numCols, memUsed=$memUsed")
      this.freeMemory(size)
      shouldSpill = true
      return size
    }
    0L // no spill triggered by other memory consumer
  }
}
