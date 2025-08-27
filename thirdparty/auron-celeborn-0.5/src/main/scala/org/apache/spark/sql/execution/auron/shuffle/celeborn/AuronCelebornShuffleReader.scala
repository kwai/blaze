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
package org.apache.spark.sql.execution.auron.shuffle.celeborn

import java.io.InputStream
import java.io.OutputStream
import java.lang.reflect.InvocationHandler
import java.lang.reflect.Method
import java.nio.ByteBuffer

import scala.reflect.ClassTag

import org.apache.celeborn.client.read.CelebornInputStream
import org.apache.celeborn.common.CelebornConf
import org.apache.commons.lang3.reflect.FieldUtils
import org.apache.spark.ShuffleDependency
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.DeserializationStream
import org.apache.spark.serializer.SerializationStream
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.shuffle.ShuffleReadMetricsReporter
import org.apache.spark.shuffle.celeborn.CelebornShuffleHandle
import org.apache.spark.shuffle.celeborn.CelebornShuffleReader
import org.apache.spark.shuffle.celeborn.ExecutorShuffleIdTracker
import org.apache.spark.sql.execution.auron.shuffle.AuronRssShuffleReaderBase

class AuronCelebornShuffleReader[K, C](
    conf: CelebornConf,
    handle: CelebornShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    startMapIndex: Option[Int] = None,
    endMapIndex: Option[Int] = None,
    context: TaskContext,
    metrics: ShuffleReadMetricsReporter,
    shuffleIdTracker: ExecutorShuffleIdTracker)
    extends AuronRssShuffleReaderBase[K, C](handle, context)
    with Logging {

  override protected def readBlocks(): Iterator[InputStream] = {
    // force disable decompression because compression is skipped in shuffle writer
    val reader = new CelebornShuffleReader[K, C](
      handle,
      startPartition,
      endPartition,
      startMapIndex.getOrElse(0),
      endMapIndex.getOrElse(Int.MaxValue),
      context,
      conf,
      AuronCelebornShuffleReader.createBypassingIncRecordsReadMetrics(metrics),
      shuffleIdTracker) {

      override def newSerializerInstance(dep: ShuffleDependency[K, _, C]): SerializerInstance = {
        new SerializerInstance {
          override def serialize[T: ClassTag](t: T): ByteBuffer =
            throw new UnsupportedOperationException(
              "AuronCelebornShuffleReader.newSerializerInstance")

          override def deserialize[T: ClassTag](bytes: ByteBuffer): T =
            throw new UnsupportedOperationException(
              "AuronCelebornShuffleReader.newSerializerInstance")

          override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T =
            throw new UnsupportedOperationException(
              "AuronCelebornShuffleReader.newSerializerInstance")

          override def serializeStream(s: OutputStream): SerializationStream =
            throw new UnsupportedOperationException(
              "AuronCelebornShuffleReader.newSerializerInstance")

          override def deserializeStream(s: InputStream): DeserializationStream = {
            new DeserializationStream {
              override def asKeyValueIterator: Iterator[(Any, Any)] = Iterator.single((null, s))

              override def readObject[T: ClassTag](): T =
                throw new UnsupportedOperationException()

              override def close(): Unit = s.close()
            }
          }
        }
      }
    }

    reader.read().map { kv =>
      val celebornInputStream = kv._2.asInstanceOf[CelebornInputStream]

      // force disable decompression because compression is skipped in shuffle writer
      val shuffleCompressionEnabledField =
        FieldUtils.getField(celebornInputStream.getClass, "shuffleCompressionEnabled", true)
      if (shuffleCompressionEnabledField != null) {
        FieldUtils.writeField(
          shuffleCompressionEnabledField,
          celebornInputStream,
          Boolean.box(false).asInstanceOf[Object],
          true)
      }
      celebornInputStream
    }
  }
}

object AuronCelebornShuffleReader {
  def createBypassingIncRecordsReadMetrics(
      metrics: ShuffleReadMetricsReporter): ShuffleReadMetricsReporter = {

    class MetricsInvocationHandler(metrics: ShuffleReadMetricsReporter)
        extends InvocationHandler {
      override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef = {
        method match {
          case m if m.getName == "incRecordsRead" => null
          case m => m.invoke(metrics, args: _*)
        }
      }
    }

    val classLoader = metrics.getClass.getClassLoader
    val proxy = java.lang.reflect.Proxy.newProxyInstance(
      classLoader,
      Array(classOf[ShuffleReadMetricsReporter]),
      new MetricsInvocationHandler(metrics))
    proxy.asInstanceOf[ShuffleReadMetricsReporter]
  }
}
