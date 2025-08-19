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

import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types._

abstract class AuronColumnVector(val dataType: DataType) extends AutoCloseable {

  override def close(): Unit

  def closeIfFreeable(): Unit = {
    close()
  }

  def hasNull: Boolean

  def numNulls: Int

  def isNullAt(rowId: Int): Boolean

  def getBoolean(rowId: Int): Boolean

  def getBooleans(rowId: Int, count: Int): Array[Boolean] = {
    val res = new Array[Boolean](count)
    var i = 0
    while (i < count) {
      if (!isNullAt(rowId + i)) res(i) = getBoolean(rowId + i)
      i += 1
    }
    res
  }

  def getByte(rowId: Int): Byte

  def getBytes(rowId: Int, count: Int): Array[Byte] = {
    val res = new Array[Byte](count)
    var i = 0
    while (i < count) {
      if (!isNullAt(rowId + i)) res(i) = getByte(rowId + i)
      i += 1
    }
    res
  }

  def getShort(rowId: Int): Short

  def getShorts(rowId: Int, count: Int): Array[Short] = {
    val res = new Array[Short](count)
    var i = 0
    while (i < count) {
      if (!isNullAt(rowId + i)) res(i) = getShort(rowId + i)
      i += 1
    }
    res
  }

  def getInt(rowId: Int): Int

  def getInts(rowId: Int, count: Int): Array[Int] = {
    val res = new Array[Int](count)
    var i = 0
    while (i < count) {
      if (!isNullAt(rowId + i)) res(i) = getInt(rowId + i)
      i += 1
    }
    res
  }

  def getLong(rowId: Int): Long

  def getLongs(rowId: Int, count: Int): Array[Long] = {
    val res = new Array[Long](count)
    var i = 0
    while (i < count) {
      if (!isNullAt(rowId + i)) res(i) = getLong(rowId + i)
      i += 1
    }
    res
  }

  def getFloat(rowId: Int): Float

  def getFloats(rowId: Int, count: Int): Array[Float] = {
    val res = new Array[Float](count)
    var i = 0
    while (i < count) {
      if (!isNullAt(rowId + i)) res(i) = getFloat(rowId + i)
      i += 1
    }
    res
  }

  def getDouble(rowId: Int): Double

  def getDoubles(rowId: Int, count: Int): Array[Double] = {
    val res = new Array[Double](count)
    var i = 0
    while (i < count) {
      if (!isNullAt(rowId + i)) res(i) = getDouble(rowId + i)
      i += 1
    }
    res
  }

  def getStruct(rowId: Int): AuronColumnarStruct = {
    if (isNullAt(rowId)) return null
    new AuronColumnarStruct(this, rowId)
  }

  def getArray(rowId: Int): AuronColumnarArray

  def getMap(ordinal: Int): AuronColumnarMap

  def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal

  def getUTF8String(rowId: Int): UTF8String

  def getBinary(rowId: Int): Array[Byte]

  def getChild(ordinal: Int): AuronColumnVector
}
