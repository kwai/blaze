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
import org.apache.arrow.vector.{IntVector, VarBinaryVector, VectorSchemaRoot}
import org.apache.arrow.vector.dictionary.DictionaryProvider
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.util.Utils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.blaze.util.Using
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.UnsafeRowSerializer
import org.apache.spark.sql.execution.blaze.arrowio.util.{ArrowUtils, ArrowWriter}
import org.apache.spark.sql.types.{BinaryType, DataType, IntegerType, StructField, StructType}
import org.apache.spark.sql.Row
import scala.collection.JavaConverters._
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.io.OutputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.util.ByteBufferInputStream

object UnsafeRowsWrapper extends Logging {

  private val dictionaryProvider: DictionaryProvider = new MapDictionaryProvider()
  private val idxSchema = {
    val schema = StructType(Seq(StructField("", IntegerType, nullable = false)))
    ArrowUtils.toArrowSchema(schema)
  }

  private val dataSchema = {
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
    rows: ArrayBuffer[InternalRow],
    numFields: Int,
    importFFIArrayPtr: Long,
    exportFFIArrayPtr: Long): Unit = {

    Using.resource(ArrowUtils.newChildAllocator(getClass.getName)) { batchAllocator =>
      Using.resources(
        VectorSchemaRoot.create(dataSchema, batchAllocator),
        VectorSchemaRoot.create(idxSchema, batchAllocator),
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
          val outputDataStream = new ByteArrayOutputStream()
          val serializer = new UnsafeRowSerializer(numFields).newInstance()
          Using(serializer.serializeStream(outputDataStream)) { ser =>
            for (idx <- 0 until importIdxRoot.getRowCount) {
              val rowIdx = importIdxArray.get(idx)
              val row = rows(rowIdx)
              ser.writeValue(row)
            }
          }

          // export serialized data as a single row batch using root allocator
          val outputWriter = ArrowWriter.create(exportDataRoot)
          outputWriter.write(InternalRow(outputDataStream.toByteArray))
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

  def deserialize(numFields: Int, dataBuffer: ByteBuffer): ArrayBuffer[InternalRow] = {
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