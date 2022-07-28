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

import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.channels.ReadableByteChannel

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.blaze.arrowio.ArrowReaderIterator
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.ByteBufferInputStream
import org.apache.spark.util.Utils
import org.apache.spark.TaskContext
import org.apache.spark.sql.execution.blaze.arrowio.ArrowWriterIterator
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.internal.SQLConf

case class FallbackToJvmExprContext(serialized: ByteBuffer) extends Logging {
  val expr: Expression = NativeConverters.deserializeExpression({
    val bytes = new Array[Byte](serialized.remaining())
    serialized.get(bytes)
    bytes
  })
  val timeZoneId: String = SparkEnv.get.conf.get(SQLConf.SESSION_LOCAL_TIMEZONE)

  val inputSchema: StructType = StructType(expr.children.map { param =>
    StructField(param.toString, param.dataType, param.nullable)
  })
  val outputSchema: StructType = StructType(
    StructField("_c0", expr.dataType, expr.nullable) :: Nil)

  def eval(batchBuffer: ByteBuffer): ReadableByteChannel = {
    val taskContext = TaskContext.get()

    Utils.tryWithResource(new ByteBufferInputStream(batchBuffer)) { in =>
      val channel = Channels.newChannel(in)
      val reader = new ArrowReaderIterator(channel, inputSchema, taskContext)

      val outputRows = reader
        .map(row => InternalRow(expr.eval(row)))

      val writer = new ArrowWriterIterator(
        outputRows,
        outputSchema,
        timeZoneId,
        taskContext,
        recordBatchSize = Int.MaxValue // ensure only one batch is generated
      )

      // read one output batch
      writer.next()
    }
  }
}
