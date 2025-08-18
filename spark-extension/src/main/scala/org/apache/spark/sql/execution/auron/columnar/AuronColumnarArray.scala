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

import org.apache.spark.sql.catalyst.expressions.SpecializedGettersReader
import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.ShortType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.unsafe.types.UTF8String

class AuronColumnarArray(data: AuronColumnVector, offset: Int, length: Int) extends ArrayData {
  override def numElements: Int = length

  private def setNullBits(arrayData: UnsafeArrayData) = {
    if (data.hasNull) for (i <- 0 until length) {
      if (data.isNullAt(offset + i)) arrayData.setNullAt(i)
    }
    arrayData
  }

  override def copy: ArrayData = {
    data.dataType match {
      case BooleanType => setNullBits(UnsafeArrayData.fromPrimitiveArray(toBooleanArray))
      case ByteType => setNullBits(UnsafeArrayData.fromPrimitiveArray(toByteArray))
      case ShortType => setNullBits(UnsafeArrayData.fromPrimitiveArray(toShortArray))
      case IntegerType | DateType =>
        setNullBits(UnsafeArrayData.fromPrimitiveArray(toIntArray))
      case LongType | TimestampType =>
        setNullBits(UnsafeArrayData.fromPrimitiveArray(toLongArray))
      case FloatType => setNullBits(UnsafeArrayData.fromPrimitiveArray(toFloatArray))
      case DoubleType => setNullBits(UnsafeArrayData.fromPrimitiveArray(toDoubleArray))
      case dt => new GenericArrayData(toObjectArray(dt)).copy
    } // ensure the elements are copied.
  }

  override def toBooleanArray: Array[Boolean] = data.getBooleans(offset, length)

  override def toByteArray: Array[Byte] = data.getBytes(offset, length)

  override def toShortArray: Array[Short] = data.getShorts(offset, length)

  override def toIntArray: Array[Int] = data.getInts(offset, length)

  override def toLongArray: Array[Long] = data.getLongs(offset, length)

  override def toFloatArray: Array[Float] = data.getFloats(offset, length)

  override def toDoubleArray: Array[Double] = data.getDoubles(offset, length)

  override def array: Array[Any] = {
    val dt = data.dataType
    val list = new Array[Any](length)
    try {
      for (i <- 0 until length) {
        if (!data.isNullAt(offset + i)) list(i) = get(i, dt)
      }
      list
    } catch {
      case e: Exception =>
        throw new RuntimeException("Could not get the array", e)
    }
  }

  override def isNullAt(ordinal: Int): Boolean = {
    if (data == null) {
      return true
    }
    data.isNullAt(offset + ordinal)
  }

  override def getBoolean(ordinal: Int): Boolean = data.getBoolean(offset + ordinal)

  override def getByte(ordinal: Int): Byte = data.getByte(offset + ordinal)

  override def getShort(ordinal: Int): Short = data.getShort(offset + ordinal)

  override def getInt(ordinal: Int): Int = data.getInt(offset + ordinal)

  override def getLong(ordinal: Int): Long = data.getLong(offset + ordinal)

  override def getFloat(ordinal: Int): Float = data.getFloat(offset + ordinal)

  override def getDouble(ordinal: Int): Double = data.getDouble(offset + ordinal)

  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal = {
    if (data == null) return null
    data.getDecimal(offset + ordinal, precision, scale)
  }

  override def getUTF8String(ordinal: Int): UTF8String = {
    if (data == null) return null
    data.getUTF8String(offset + ordinal)
  }

  override def getBinary(ordinal: Int): Array[Byte] = {
    if (data == null) return null
    data.getBinary(offset + ordinal)
  }

  override def getStruct(ordinal: Int, numFields: Int): AuronColumnarStruct = {
    if (data == null) return null
    data.getStruct(offset + ordinal)
  }

  override def getArray(ordinal: Int): AuronColumnarArray = {
    if (data == null) return null
    data.getArray(offset + ordinal)
  }

  override def getInterval(ordinal: Int): CalendarInterval = {
    throw new UnsupportedOperationException
  }

  override def getMap(ordinal: Int): AuronColumnarMap = {
    if (data == null) return null
    data.getMap(offset + ordinal)
  }

  override def get(ordinal: Int, dataType: DataType): AnyRef = {
    if (data == null) return null
    SpecializedGettersReader.read(this, ordinal, dataType, true, false)
  }

  override def update(ordinal: Int, value: Any): Unit = {
    throw new UnsupportedOperationException
  }

  override def setNullAt(ordinal: Int): Unit = {
    throw new UnsupportedOperationException
  }
}
