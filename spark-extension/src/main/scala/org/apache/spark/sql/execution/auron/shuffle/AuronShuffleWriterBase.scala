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
package org.apache.spark.sql.execution.auron.shuffle

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.file.{Files, Paths}

import org.apache.spark.{Partition, ShuffleDependency, SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{IndexShuffleBlockResolver, ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.sql.auron.{NativeHelper, NativeRDD, Shims}

import org.apache.auron.protobuf.{PhysicalPlanNode, ShuffleWriterExecNode}

abstract class AuronShuffleWriterBase[K, V](metrics: ShuffleWriteMetricsReporter)
    extends ShuffleWriter[K, V]
    with Logging {

  protected var partitionLengths: Array[Long] = Array[Long]()
  private var mapStatus: Option[MapStatus] = None

  override def write(records: Iterator[Product2[K, V]]): Unit = {}

  def nativeShuffleWrite(
      nativeShuffleRDD: NativeRDD,
      dep: ShuffleDependency[_, _, _],
      mapId: Int,
      context: TaskContext,
      partition: Partition): Unit = {

    val shuffleBlockResolver =
      SparkEnv.get.shuffleManager.shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver]
    val dataFile = shuffleBlockResolver.getDataFile(dep.shuffleId, mapId)
    val tempDataFilename = dataFile.getPath.replace(".data", ".data.tmp")
    val tempIndexFilename = dataFile.getPath.replace(".data", ".index.tmp")
    val tempDataFilePath = Paths.get(tempDataFilename)
    val tempIndexFilePath = Paths.get(tempIndexFilename)

    val nativeShuffleWriterExec = PhysicalPlanNode
      .newBuilder()
      .setShuffleWriter(
        ShuffleWriterExecNode
          .newBuilder(nativeShuffleRDD.nativePlan(partition, context).getShuffleWriter)
          .setOutputDataFile(tempDataFilename)
          .setOutputIndexFile(tempIndexFilename)
          .build())
      .build()
    val iterator = NativeHelper.executeNativePlan(
      nativeShuffleWriterExec,
      nativeShuffleRDD.metrics,
      partition,
      Some(context))
    assert(iterator.toArray.isEmpty)

    // get partition lengths from shuffle write output index file
    var offset = 0L
    partitionLengths = Files
      .readAllBytes(tempIndexFilePath)
      .grouped(8)
      .drop(1) // first partition offset is always 0
      .map(indexBytes => {
        val partitionOffset =
          ByteBuffer.wrap(indexBytes).order(ByteOrder.LITTLE_ENDIAN).getLong
        val partitionLength = partitionOffset - offset
        offset = partitionOffset
        partitionLength
      })
      .toArray

    // update metrics
    val dataSize = Files.size(tempDataFilePath)
    metrics.incBytesWritten(dataSize)

    mapStatus = Some(
      Shims.get.commit(
        dep,
        shuffleBlockResolver,
        tempDataFilePath.toFile,
        mapId,
        partitionLengths,
        dataSize,
        context))
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    mapStatus.filter(_ => success)
  }
}
