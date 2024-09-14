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
package org.apache.spark.sql.execution.blaze.shuffle

import java.util.UUID

import org.apache.spark.SparkEnv
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.sql.blaze.JniBridge
import org.apache.spark.sql.blaze.NativeHelper
import org.apache.spark.sql.blaze.NativeRDD
import org.apache.spark.sql.blaze.Shims
import org.apache.spark.Partition
import org.apache.spark.ShuffleDependency
import org.apache.spark.TaskContext
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.ShuffleHandle
import org.blaze.protobuf.PhysicalPlanNode
import org.blaze.protobuf.RssShuffleWriterExecNode

abstract class BlazeRssShuffleWriterBase[K, V](metrics: ShuffleWriteMetricsReporter)
    extends BlazeShuffleWriterBase[K, V](metrics) {

  def getRssPartitionWriter(
      handle: ShuffleHandle,
      mapId: Int,
      metrics: ShuffleWriteMetricsReporter,
      numPartitions: Int): RssPartitionWriterBase

  def nativeRssShuffleWrite(
      nativeShuffleRDD: NativeRDD,
      dep: ShuffleDependency[_, _, _],
      mapId: Int,
      context: TaskContext,
      partition: Partition,
      numPartitions: Int): MapStatus = {

    val rssShuffleWriterObject =
      getRssPartitionWriter(dep.shuffleHandle, mapId, metrics, numPartitions)
    if (rssShuffleWriterObject == null) {
      throw new RuntimeException("cannot get RssPartitionWriter")
    }

    try {
      val jniResourceId = s"RssPartitionWriter:${UUID.randomUUID().toString}"
      JniBridge.resourcesMap.put(jniResourceId, rssShuffleWriterObject)
      val nativeRssShuffleWriterExec = PhysicalPlanNode
        .newBuilder()
        .setRssShuffleWriter(
          RssShuffleWriterExecNode
            .newBuilder(nativeShuffleRDD.nativePlan(partition, context).getRssShuffleWriter)
            .setRssPartitionWriterResourceId(jniResourceId)
            .build())
        .build()

      val iterator = NativeHelper.executeNativePlan(
        nativeRssShuffleWriterExec,
        nativeShuffleRDD.metrics,
        partition,
        Some(context))
      assert(iterator.toArray.isEmpty)
    } finally {
      rssShuffleWriterObject.close()
    }

    val mapStatus = Shims.get.getMapStatus(
      SparkEnv.get.blockManager.shuffleServerId,
      rssShuffleWriterObject.getPartitionLengthMap,
      mapId)
    mapStatus
  }
}
