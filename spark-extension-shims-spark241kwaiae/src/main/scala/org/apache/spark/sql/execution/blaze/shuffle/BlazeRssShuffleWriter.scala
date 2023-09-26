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

import org.apache.spark.{Partition, ShuffleDependency, SparkEnv, TaskContext}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.sql.blaze.{JniBridge, NativeHelper, NativeRDD, Shims}
import org.blaze.protobuf.{PhysicalPlanNode, RssShuffleWriterExecNode}

import java.util.UUID

class BlazeRssShuffleWriter[K, V](metrics: ShuffleWriteMetricsReporter)
    extends BlazeShuffleWriterBase[K, V](metrics) {

  def nativeRssShuffleWrite(
      nativeShuffleRDD: NativeRDD,
      dep: ShuffleDependency[_, _, _],
      mapId: Int,
      context: TaskContext,
      partition: Partition,
      numPartitions: Int): MapStatus = {

    val rssShuffleWriterObject = Shims.get
      .getRssPartitionWriter(dep.shuffleHandle, mapId, metrics, numPartitions)
    assert(rssShuffleWriterObject.isDefined)
    try {
      val jniResourceId = s"RssPartitionWriter:${UUID.randomUUID().toString}"
      JniBridge.resourcesMap.put(jniResourceId, rssShuffleWriterObject.get)
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
      rssShuffleWriterObject.get.close()
    }

    val mapStatus = Shims.get.getMapStatus(
      SparkEnv.get.blockManager.shuffleServerId,
      rssShuffleWriterObject.get.getPartitionLengthMap,
      mapId)
    mapStatus
  }

}
