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

import org.apache.arrow.c.{ArrowArray, Data}
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.dictionary.DictionaryProvider
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.util.Utils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.blaze.util.Using
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.UnsafeRowSerializer
import org.apache.spark.sql.execution.blaze.arrowio.util.{ArrowUtils, ArrowWriter}
import org.apache.spark.sql.types.{BinaryType, DataType, IntegerType, LongType, StructField, StructType}
import org.apache.arrow.flatbuf
import com.google.flatbuffers.{IntVector, LongVector}
import org.apache.spark.sql.Row

import scala.collection.JavaConverters._
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.lang.reflect.Field

object UnsafeRowsWrapper extends Logging {

  private val dictionaryProvider: DictionaryProvider = new MapDictionaryProvider()
  private val idxSchema = {
    val schema = StructType(Seq(StructField("", LongType, nullable = false)))
    ArrowUtils.toArrowSchema(schema)
  }

  private val byteSchema = {
    val schema = StructType(Seq(StructField("", BinaryType, nullable = false)))
    ArrowUtils.toArrowSchema(schema)
  }

  private val deserializeSchema = {
    val schema = StructType(
      Seq(
        StructField("", BinaryType, nullable = false),
        StructField("", IntegerType, nullable = false)))
    ArrowUtils.toArrowSchema(schema)
  }

  private val offsetSchema = {
    val schema = StructType(Seq(StructField("", IntegerType, nullable = false)))
    ArrowUtils.toArrowSchema(schema)
  }

  private def toUnsafeRow(row: Row, schema: Array[DataType]): UnsafeRow = {
    val converter = unsafeRowConverter(schema)
    converter(row)
  }

  private def unsafeRowConverter(schema: Array[DataType]): Row => UnsafeRow = {
    val converter = UnsafeProjection.create(schema)
    (row: Row) => {
      converter(CatalystTypeConverters.convertToCatalyst(row).asInstanceOf[InternalRow])
    }
  }

  def serialize(
      unsafeRows: Array[InternalRow],
      numFields: Int,
      importFFIArrayPtr: Long,
      exportFFIArrayPtr: Long): Unit = {
    Using.resource(ArrowUtils.newChildAllocator(getClass.getName)) { batchAllocator =>
      Using.resources(
        VectorSchemaRoot.create(byteSchema, batchAllocator),
        VectorSchemaRoot.create(idxSchema, batchAllocator),
        ArrowArray.wrap(importFFIArrayPtr),
        ArrowArray.wrap(exportFFIArrayPtr)) {
        (outputRoot, paramsRoot, importArray, exportArray) =>
          // import into params root
          Data.importIntoVectorSchemaRoot(
            batchAllocator,
            importArray,
            paramsRoot,
            dictionaryProvider)
          val idxArray = paramsRoot.getFieldVectors.asScala.head.asInstanceOf[LongVector];

          val serializer = new UnsafeRowSerializer(numFields).newInstance()
          val outputWriter = ArrowWriter.create(outputRoot)
          for (idx <- 0 until idxArray.length()) {
            val internalRow = unsafeRows(idx)
            Utils.tryWithResource(new ByteArrayOutputStream()) { baos =>
              val serializerStream = serializer.serializeStream(baos)
              serializerStream.writeValue(internalRow)
              val bytes = baos.toByteArray
              outputWriter.write(toUnsafeRow(Row(bytes), Array(BinaryType)))
            }
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

  def deserialize(
      numFields: Int,
      importFFIArrayPtr: Long,
      exportFFIArrayPtr: Long): Array[InternalRow] = {

    Using.resource(ArrowUtils.newChildAllocator(getClass.getName)) { batchAllocator =>
      Using.resources(
        VectorSchemaRoot.create(deserializeSchema, batchAllocator),
        VectorSchemaRoot.create(offsetSchema, batchAllocator),
        ArrowArray.wrap(importFFIArrayPtr),
        ArrowArray.wrap(exportFFIArrayPtr)) {
        (paramsRoot, outputRoot, importArray, exportArray) =>
          Data.importIntoVectorSchemaRoot(
            batchAllocator,
            importArray,
            paramsRoot,
            dictionaryProvider)
          val fieldVectors = paramsRoot.getFieldVectors.asScala
          val binaryVector = fieldVectors.head.asInstanceOf[flatbuf.Binary.Vector];
          val intVector = fieldVectors(1).asInstanceOf[IntVector]

          assert(
            binaryVector.length() == intVector.length(),
            s"Error: UnsafeRowsWrapper deserialize error Vectors have different lengths.")

          val deserializer = new UnsafeRowSerializer(numFields).newInstance()
          val internalRowsArray = new Array[InternalRow](binaryVector.length())
          val outputWriter = ArrowWriter.create(outputRoot)
          for (i <- 0 until binaryVector.length()) {
            val binaryRow = binaryVector.get(i)
            val offset = intVector.get(i)
            val bytes = binaryRow.getByteBuffer.array()
            val internalRow: InternalRow = Utils.tryWithResource(
              new ByteArrayInputStream(bytes, offset, bytes.length - offset)) { bais =>
              val unsafeRow =
                deserializer.deserializeStream(bais).readValue().asInstanceOf[UnsafeRow]
              // get offset use reflect
              val field: Field = classOf[ByteArrayInputStream].getDeclaredField("pos")
              field.setAccessible(true)
              val position = field.getInt(bais)
              outputWriter.write(toUnsafeRow(Row(position), Array(IntegerType)))
              unsafeRow
            }
            internalRowsArray(i) = internalRow
          }

          outputWriter.finish()

          // export to output using root allocator
          Data.exportVectorSchemaRoot(
            ArrowUtils.rootAllocator,
            outputRoot,
            dictionaryProvider,
            exportArray)

          internalRowsArray
      }
    }
  }

  def getRowNum(unsafeRows: Array[InternalRow]): Int = {
    unsafeRows.length
  }

  def getNullObject(rowNum: Int): Array[InternalRow] = {
    Array.fill(rowNum)(InternalRow.empty)
  }

}
