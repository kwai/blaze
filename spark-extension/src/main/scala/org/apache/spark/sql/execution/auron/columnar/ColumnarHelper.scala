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
package org.apache.spark.sql.execution.auron.columnar

import scala.collection.JavaConverters._

import org.apache.arrow.vector.VectorSchemaRoot

object ColumnarHelper {
  def rootRowsIter(root: VectorSchemaRoot): Iterator[AuronColumnarBatchRow] = {
    val row = rootRowReusable(root)
    val numRows = root.getRowCount
    Range(0, numRows).iterator.map { rowId =>
      row.rowId = rowId
      row
    }
  }

  def rootRowReusable(root: VectorSchemaRoot): AuronColumnarBatchRow = {
    val vectors = root.getFieldVectors.asScala.toArray
    new AuronColumnarBatchRow(
      vectors.map(new AuronArrowColumnVector(_).asInstanceOf[AuronColumnVector]))
  }
}
