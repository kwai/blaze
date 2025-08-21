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

import org.apache.arrow.vector.BigIntVector
import org.apache.arrow.vector.BitVector
import org.apache.arrow.vector.DateDayVector
import org.apache.arrow.vector.DecimalVector
import org.apache.arrow.vector.Float4Vector
import org.apache.arrow.vector.Float8Vector
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.NullVector
import org.apache.arrow.vector.SmallIntVector
import org.apache.arrow.vector.TimeStampMicroTZVector
import org.apache.arrow.vector.TimeStampMicroVector
import org.apache.arrow.vector.TinyIntVector
import org.apache.arrow.vector.UInt1Vector
import org.apache.arrow.vector.UInt2Vector
import org.apache.arrow.vector.UInt4Vector
import org.apache.arrow.vector.UInt8Vector
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.VarBinaryVector
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.complex.BaseRepeatedValueVector
import org.apache.arrow.vector.complex.FixedSizeListVector
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.complex.MapVector
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.holders.NullableVarCharHolder
import org.apache.spark.sql.execution.auron.arrowio.util.ArrowUtils
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.UTF8String

class AuronArrowColumnVector(vector: ValueVector)
    extends AuronColumnVector(ArrowUtils.fromArrowField(vector.getField)) {
  private var childColumns: Array[AuronArrowColumnVector] = _
  private val accessor = vector match {
    case v: BitVector => new AuronArrowColumnVector.BooleanAccessor(v)
    case v: UInt1Vector => new AuronArrowColumnVector.UInt1Accessor(v)
    case v: UInt2Vector => new AuronArrowColumnVector.UInt2Accessor(v)
    case v: UInt4Vector => new AuronArrowColumnVector.UInt4Accessor(v)
    case v: UInt8Vector => new AuronArrowColumnVector.UInt8Accessor(v)
    case v: TinyIntVector => new AuronArrowColumnVector.ByteAccessor(v)
    case v: SmallIntVector => new AuronArrowColumnVector.ShortAccessor(v)
    case v: IntVector => new AuronArrowColumnVector.IntAccessor(v)
    case v: BigIntVector => new AuronArrowColumnVector.LongAccessor(v)
    case v: Float4Vector => new AuronArrowColumnVector.FloatAccessor(v)
    case v: Float8Vector => new AuronArrowColumnVector.DoubleAccessor(v)
    case v: DecimalVector => new AuronArrowColumnVector.DecimalAccessor(v)
    case v: VarCharVector => new AuronArrowColumnVector.StringAccessor(v)
    case v: VarBinaryVector => new AuronArrowColumnVector.BinaryAccessor(v)
    case v: DateDayVector => new AuronArrowColumnVector.DateAccessor(v)
    case v: TimeStampMicroVector => new AuronArrowColumnVector.TimestampAccessor(v)
    case v: TimeStampMicroTZVector => new AuronArrowColumnVector.TimestampTZAccessor(v)
    case mapVector: MapVector =>
      new AuronArrowColumnVector.MapAccessor(mapVector)
    case listVector: ListVector =>
      new AuronArrowColumnVector.ArrayAccessor(listVector)
    case listVector: FixedSizeListVector =>
      new AuronArrowColumnVector.FixedSizeArrayAccessor(listVector)
    case structVector: StructVector =>
      val accessor = new AuronArrowColumnVector.StructAccessor(structVector)
      childColumns = new Array[AuronArrowColumnVector](structVector.size)
      for (i <- childColumns.indices) {
        childColumns(i) = new AuronArrowColumnVector(structVector.getVectorById(i))
      }
      accessor
    case v: NullVector => new AuronArrowColumnVector.NullAccessor(v)
    case v => throw new UnsupportedOperationException("unsupported vector type: " + v.getClass)
  }

  override def hasNull: Boolean = vector.getNullCount > 0

  override def numNulls: Int = vector.getNullCount

  override def close(): Unit = {
    if (childColumns != null) {
      for (i <- childColumns.indices) {
        childColumns(i).close()
        childColumns(i) = null
      }
      childColumns = null
    }
    accessor.close()
  }

  override def isNullAt(rowId: Int): Boolean = accessor.isNullAt(rowId)

  override def getBoolean(rowId: Int): Boolean = accessor.getBoolean(rowId)

  override def getByte(rowId: Int): Byte = accessor.getByte(rowId)

  override def getShort(rowId: Int): Short = accessor.getShort(rowId)

  override def getInt(rowId: Int): Int = accessor.getInt(rowId)

  override def getLong(rowId: Int): Long = accessor.getLong(rowId)

  override def getFloat(rowId: Int): Float = accessor.getFloat(rowId)

  override def getDouble(rowId: Int): Double = accessor.getDouble(rowId)

  override def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal = {
    if (isNullAt(rowId)) return null
    accessor.getDecimal(rowId, precision, scale)
  }

  override def getUTF8String(rowId: Int): UTF8String = {
    if (isNullAt(rowId)) return null
    accessor.getUTF8String(rowId)
  }

  override def getBinary(rowId: Int): Array[Byte] = {
    if (isNullAt(rowId)) return null
    accessor.getBinary(rowId)
  }

  override def getArray(rowId: Int): AuronColumnarArray = {
    if (isNullAt(rowId)) return null
    accessor.getArray(rowId)
  }

  override def getMap(rowId: Int): AuronColumnarMap = {
    if (isNullAt(rowId)) return null
    accessor.getMap(rowId)
  }

  override def getChild(ordinal: Int): AuronArrowColumnVector = childColumns(ordinal)
}

object AuronArrowColumnVector {
  abstract private class ArrowVectorAccessor(private val vector: ValueVector) {
    def isNullAt(rowId: Int): Boolean =
      if (vector.getValueCount > 0 && vector.getValidityBuffer.capacity == 0) false
      else vector.isNull(rowId)

    final def getNullCount: Int = vector.getNullCount

    final def close(): Unit = {
      vector.close()
    }

    def getBoolean(rowId: Int): Boolean = throw new UnsupportedOperationException(
      this.getClass.getName)

    def getByte(rowId: Int): Byte = throw new UnsupportedOperationException(this.getClass.getName)

    def getShort(rowId: Int): Short = throw new UnsupportedOperationException(
      this.getClass.getName)

    def getInt(rowId: Int): Int = throw new UnsupportedOperationException(this.getClass.getName)

    def getLong(rowId: Int): Long = throw new UnsupportedOperationException(this.getClass.getName)

    def getFloat(rowId: Int): Float = throw new UnsupportedOperationException(
      this.getClass.getName)

    def getDouble(rowId: Int): Double = throw new UnsupportedOperationException(
      this.getClass.getName)

    def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal =
      throw new UnsupportedOperationException(this.getClass.getName)

    def getUTF8String(rowId: Int): UTF8String = throw new UnsupportedOperationException(
      this.getClass.getName)

    def getBinary(rowId: Int): Array[Byte] = throw new UnsupportedOperationException(
      this.getClass.getName)

    def getArray(rowId: Int): AuronColumnarArray = throw new UnsupportedOperationException(
      this.getClass.getName)

    def getMap(rowId: Int): AuronColumnarMap = throw new UnsupportedOperationException(
      this.getClass.getName)
  }

  private class NullAccessor(vector: NullVector)
      extends AuronArrowColumnVector.ArrowVectorAccessor(vector) {
    override def isNullAt(rowId: Int) = true
  }

  private class BooleanAccessor(vector: BitVector)
      extends AuronArrowColumnVector.ArrowVectorAccessor(vector) {
    final override def getBoolean(rowId: Int): Boolean = vector.get(rowId) == 1
  }

  private class ByteAccessor(vector: TinyIntVector)
      extends AuronArrowColumnVector.ArrowVectorAccessor(vector) {
    final override def getByte(rowId: Int): Byte = vector.get(rowId)
  }

  private class UInt1Accessor(vector: UInt1Vector)
      extends AuronArrowColumnVector.ArrowVectorAccessor(vector) {
    final override def getByte(rowId: Int): Byte = vector.get(rowId)
  }

  private class UInt2Accessor(vector: UInt2Vector)
      extends AuronArrowColumnVector.ArrowVectorAccessor(vector) {
    final override def getShort(rowId: Int): Short = vector.get(rowId).toShort
  }

  private class UInt4Accessor(vector: UInt4Vector)
      extends AuronArrowColumnVector.ArrowVectorAccessor(vector) {
    final override def getInt(rowId: Int) = vector.get(rowId)
  }

  private class UInt8Accessor(vector: UInt8Vector)
      extends AuronArrowColumnVector.ArrowVectorAccessor(vector) {
    final override def getLong(rowId: Int): Long = vector.get(rowId)
  }

  private class ShortAccessor(vector: SmallIntVector)
      extends AuronArrowColumnVector.ArrowVectorAccessor(vector) {
    final override def getShort(rowId: Int): Short = vector.get(rowId)
  }

  private class IntAccessor(vector: IntVector)
      extends AuronArrowColumnVector.ArrowVectorAccessor(vector) {
    final override def getInt(rowId: Int): Int = vector.get(rowId)
  }

  private class LongAccessor(vector: BigIntVector)
      extends AuronArrowColumnVector.ArrowVectorAccessor(vector) {
    final override def getLong(rowId: Int): Long = vector.get(rowId)
  }

  private class FloatAccessor(vector: Float4Vector)
      extends AuronArrowColumnVector.ArrowVectorAccessor(vector) {
    final override def getFloat(rowId: Int): Float = vector.get(rowId)
  }

  private class DoubleAccessor(vector: Float8Vector)
      extends AuronArrowColumnVector.ArrowVectorAccessor(vector) {
    final override def getDouble(rowId: Int): Double = vector.get(rowId)
  }

  private class DecimalAccessor(vector: DecimalVector)
      extends AuronArrowColumnVector.ArrowVectorAccessor(vector) {
    final override def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal = {
      if (isNullAt(rowId)) return null
      Decimal.apply(vector.getObject(rowId), precision, scale)
    }
  }

  private class StringAccessor(vector: VarCharVector)
      extends AuronArrowColumnVector.ArrowVectorAccessor(vector) {
    final private val stringResult = new NullableVarCharHolder

    final override def getUTF8String(rowId: Int) = {
      vector.get(rowId, stringResult)
      if (stringResult.isSet == 0) null
      else
        UTF8String.fromAddress(
          null,
          stringResult.buffer.memoryAddress + stringResult.start,
          stringResult.end - stringResult.start)
    }
  }

  private class BinaryAccessor(vector: VarBinaryVector)
      extends AuronArrowColumnVector.ArrowVectorAccessor(vector) {
    final override def getBinary(rowId: Int): Array[Byte] = vector.getObject(rowId)
  }

  private class DateAccessor(vector: DateDayVector)
      extends AuronArrowColumnVector.ArrowVectorAccessor(vector) {
    final override def getInt(rowId: Int): Int = vector.get(rowId)
  }

  private class TimestampAccessor(vector: TimeStampMicroVector)
      extends AuronArrowColumnVector.ArrowVectorAccessor(vector) {
    final override def getLong(rowId: Int): Long = vector.get(rowId)
  }

  private class TimestampTZAccessor(vector: TimeStampMicroTZVector)
      extends AuronArrowColumnVector.ArrowVectorAccessor(vector) {
    final override def getLong(rowId: Int): Long = vector.get(rowId)
  }

  private class ArrayAccessor(vector: ListVector)
      extends AuronArrowColumnVector.ArrowVectorAccessor(vector) {
    private val arrayData = new AuronArrowColumnVector(vector.getDataVector)

    final override def getArray(rowId: Int): AuronColumnarArray = {
      val start = vector.getElementStartIndex(rowId)
      val end = vector.getElementEndIndex(rowId)
      new AuronColumnarArray(arrayData, start, end - start)
    }
  }

  private class FixedSizeArrayAccessor(vector: FixedSizeListVector)
      extends AuronArrowColumnVector.ArrowVectorAccessor(vector) {
    private val arrayData = new AuronArrowColumnVector(vector.getDataVector)

    final override def getArray(rowId: Int): AuronColumnarArray = {
      val start = vector.getElementStartIndex(rowId)
      val end = vector.getElementEndIndex(rowId)
      new AuronColumnarArray(arrayData, start, end - start)
    }
  }

  private class StructAccessor(vector: StructVector)
      extends AuronArrowColumnVector.ArrowVectorAccessor(vector) {}

  private class MapAccessor(vector: MapVector)
      extends AuronArrowColumnVector.ArrowVectorAccessor(vector) {
    private val entries = vector.getDataVector.asInstanceOf[StructVector]
    private val keys = new AuronArrowColumnVector(entries.getChild(MapVector.KEY_NAME))
    private val values = new AuronArrowColumnVector(entries.getChild(MapVector.VALUE_NAME))

    final override def getMap(rowId: Int): AuronColumnarMap = {
      val index = rowId * BaseRepeatedValueVector.OFFSET_WIDTH
      val offset = vector.getOffsetBuffer.getInt(index)
      val length = vector.getInnerValueCountAt(rowId)
      new AuronColumnarMap(keys, values, offset, length)
    }
  }
}
