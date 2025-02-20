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
package org.apache.spark.sql.execution.blaze.columnar

import scala.collection.JavaConverters._

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.sql.catalyst.InternalRow

object ColumnarHelper {
  def rootRowsIter(root: VectorSchemaRoot): Iterator[InternalRow] = {
    val vectors = root.getFieldVectors.asScala.toArray
    val numRows = root.getRowCount
    val row = new BlazeColumnarBatchRow(
      vectors.map(new BlazeArrowColumnVector(_).asInstanceOf[BlazeColumnVector]))

    Range(0, numRows).iterator.map { rowId =>
      row.rowId = rowId
      row.asInstanceOf[InternalRow]
    }
  }

  def rootRowsArray(root: VectorSchemaRoot): Array[InternalRow] = {
    val vectors = root.getFieldVectors.asScala.toArray
    val numRows = root.getRowCount
    val row = new BlazeColumnarBatchRow(
      vectors.map(new BlazeArrowColumnVector(_).asInstanceOf[BlazeColumnVector])
    )
    (0 until numRows).map { rowId =>
      row.rowId = rowId
      row.asInstanceOf[InternalRow]
    }.toArray
  }

}
