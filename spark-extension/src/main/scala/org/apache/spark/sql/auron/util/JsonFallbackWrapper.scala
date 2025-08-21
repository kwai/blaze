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
package org.apache.spark.sql.auron.util

import org.apache.arrow.c.{ArrowArray, Data}
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{BoundReference, GenericInternalRow, GetJsonObject, Literal}
import org.apache.spark.sql.execution.auron.arrowio.util.{ArrowUtils, ArrowWriter}
import org.apache.spark.sql.execution.auron.arrowio.util.ArrowUtils.ROOT_ALLOCATOR
import org.apache.spark.sql.execution.auron.columnar.ColumnarHelper
import org.apache.spark.sql.types.{StringType, StructField, StructType}

case class JsonFallbackWrapper(jsonPath: String) extends Logging {
  private val singleFieldJavaSchema = StructType(
    Seq(StructField("", StringType, nullable = true)))
  private val singleFieldSchema = ArrowUtils.toArrowSchema(singleFieldJavaSchema)
  private val dictionaryProvider = new MapDictionaryProvider()
  private val getJsonObjectExpr =
    GetJsonObject(BoundReference(0, StringType, nullable = false), Literal(jsonPath))

  def parseJsons(jsonsStrArrayPtr: Long, outputArrayPtr: Long): Unit = {
    Using.resources(
      VectorSchemaRoot.create(singleFieldSchema, ROOT_ALLOCATOR),
      VectorSchemaRoot.create(singleFieldSchema, ROOT_ALLOCATOR),
      ArrowArray.wrap(jsonsStrArrayPtr),
      ArrowArray.wrap(outputArrayPtr)) { case (inputRoot, outputRoot, inputArray, outputArray) =>
      Data.importIntoVectorSchemaRoot(ROOT_ALLOCATOR, inputArray, inputRoot, dictionaryProvider)
      val inputRow = ColumnarHelper.rootRowReusable(inputRoot)
      val nullOutputRow = new GenericInternalRow(1)
      val outputRow = new GenericInternalRow(1)
      val outputWriter = ArrowWriter.create(outputRoot)
      for (i <- 0 until inputRoot.getRowCount) {
        inputRow.rowId = i
        getJsonObjectExpr.eval(inputRow) match {
          case null =>
            outputWriter.write(nullOutputRow)
          case value =>
            outputRow.update(0, value)
            outputWriter.write(outputRow)
        }
      }
      outputWriter.finish()
      Data.exportVectorSchemaRoot(ROOT_ALLOCATOR, outputRoot, dictionaryProvider, outputArray)
    }
  }
}
