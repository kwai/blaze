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
package org.apache.spark.sql.execution.blaze.plan

import org.apache.spark.sql.blaze.NativeConverters
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.blaze.{protobuf => pb}

object Util {
  def getSchema[E <: NamedExpression](
      fieldItems: Seq[E],
      useExprId: Boolean = true): StructType = {
    StructType(fieldItems.map { item =>
      val name = if (useExprId) {
        getFieldNameByExprId(item)
      } else {
        item.name
      }
      StructField(name, item.dataType, item.nullable, item.metadata)
    })
  }

  def getNativeSchema[E <: NamedExpression](
      fieldItems: Seq[E],
      useExprId: Boolean = true): pb.Schema = {
    NativeConverters.convertSchema(getSchema(fieldItems, useExprId))
  }

  def getFieldNameByExprId(expr: NamedExpression): String =
    s"#${expr.exprId.id}"
}
