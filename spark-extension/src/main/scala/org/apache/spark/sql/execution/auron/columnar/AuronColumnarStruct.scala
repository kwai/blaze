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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGettersReader
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.MapType
import org.apache.spark.sql.types.ShortType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.unsafe.types.UTF8String

class AuronColumnarStruct(data: AuronColumnVector, rowId: Int) extends InternalRow {
  override def numFields: Int = data.dataType.asInstanceOf[StructType].size

  override def copy: GenericInternalRow = {
    val row = new GenericInternalRow(numFields)
    for (i <- 0 until numFields) {
      if (isNullAt(i)) row.setNullAt(i)
      else {
        val dt = data.getChild(i).dataType
        dt match {
          case BooleanType => row.setBoolean(i, getBoolean(i))
          case ByteType => row.setByte(i, getByte(i))
          case ShortType => row.setShort(i, getShort(i))
          case IntegerType => row.setInt(i, getInt(i))
          case LongType => row.setLong(i, getLong(i))
          case FloatType => row.setFloat(i, getFloat(i))
          case DoubleType => row.setDouble(i, getDouble(i))
          case StringType => row.update(i, getUTF8String(i).copy)
          case BinaryType => row.update(i, getBinary(i))
          case t: DecimalType =>
            row.setDecimal(i, getDecimal(i, t.precision, t.scale), t.precision)
          case t: StructType =>
            row.update(i, getStruct(i, t.fields.length).copy)
          case _: ArrayType => row.update(i, getArray(i).copy)
          case _: MapType => row.update(i, getMap(i).copy)
          case _ => throw new RuntimeException("Not implemented. " + dt)
        }
      }
    }
    row
  }

  override def anyNull: Boolean = throw new UnsupportedOperationException

  override def isNullAt(ordinal: Int): Boolean = {
    if (data == null) {
      return true
    }

    val c = data.getChild(ordinal)
    if (c == null) {
      return true
    }
    c.isNullAt(rowId)
  }

  override def getBoolean(ordinal: Int): Boolean = data.getChild(ordinal).getBoolean(rowId)

  override def getByte(ordinal: Int): Byte = data.getChild(ordinal).getByte(rowId)

  override def getShort(ordinal: Int): Short = data.getChild(ordinal).getShort(rowId)

  override def getInt(ordinal: Int): Int = data.getChild(ordinal).getInt(rowId)

  override def getLong(ordinal: Int): Long = data.getChild(ordinal).getLong(rowId)

  override def getFloat(ordinal: Int): Float = data.getChild(ordinal).getFloat(rowId)

  override def getDouble(ordinal: Int): Double = data.getChild(ordinal).getDouble(rowId)

  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal = {
    if (data == null) return null
    data.getChild(ordinal).getDecimal(rowId, precision, scale)
  }

  override def getUTF8String(ordinal: Int): UTF8String = {
    if (data == null) return null
    data.getChild(ordinal).getUTF8String(rowId)
  }

  override def getBinary(ordinal: Int): Array[Byte] = {
    if (data == null) return null
    data.getChild(ordinal).getBinary(rowId)
  }

  override def getStruct(ordinal: Int, numFields: Int): AuronColumnarStruct = {
    if (data == null) return null
    data.getChild(ordinal).getStruct(rowId)
  }

  override def getArray(ordinal: Int): AuronColumnarArray = {
    if (data == null) return null
    data.getChild(ordinal).getArray(rowId)
  }

  override def getMap(ordinal: Int): AuronColumnarMap = {
    if (data == null) return null
    data.getChild(ordinal).getMap(rowId)
  }

  override def getInterval(ordinal: Int): CalendarInterval = {
    throw new UnsupportedOperationException
  }

  override def get(ordinal: Int, dataType: DataType): AnyRef = {
    if (data == null) return null
    SpecializedGettersReader.read(this, ordinal, dataType, true, true)
  }

  override def update(ordinal: Int, value: Any): Unit = {
    throw new UnsupportedOperationException
  }

  override def setNullAt(ordinal: Int): Unit = {
    throw new UnsupportedOperationException
  }
}
