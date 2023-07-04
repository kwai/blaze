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

package org.apache.spark.sql.blaze.shims

import java.io.File
import org.apache.spark.ShuffleDependency
import org.apache.spark.SparkEnv
import org.apache.spark.TaskContext
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{
  IndexShuffleBlockResolver,
  ShuffleHandle,
  ShuffleWriteMetricsReporter
}
import org.apache.spark.sql.blaze.ShuffleShims
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.blaze.plan.NativeShuffleExchangeBase
import org.apache.spark.sql.execution.blaze.plan.NativeShuffleExchangeExec
import org.apache.spark.sql.execution.blaze.shuffle.RssPartitionWriterBase
import org.apache.spark.storage.{BlockManagerId, FileSegment}
import org.blaze.protobuf.{PhysicalHashRepartition, PhysicalPlanNode, ShuffleWriterExecNode}

class ShuffleShimsImpl extends ShuffleShims {
  override def createArrowShuffleExchange(
      outputPartitioning: Partitioning,
      child: SparkPlan): NativeShuffleExchangeBase =
    NativeShuffleExchangeExec(outputPartitioning, child)

  // 3.0.3 does not need numRecords,just skip

  override def createFileSegment(
      file: File,
      offset: Long,
      length: Long,
      numRecords: Long): FileSegment = new FileSegment(file, offset, length)

  override def commit(
      dep: ShuffleDependency[_, _, _],
      shuffleBlockResolver: IndexShuffleBlockResolver,
      tempDataFile: File,
      mapId: Long,
      partitionLengths: Array[Long],
      dataSize: Long,
      context: TaskContext): MapStatus = {

    shuffleBlockResolver.writeIndexFileAndCommit(
      dep.shuffleId,
      mapId,
      partitionLengths,
      tempDataFile)
    MapStatus.apply(SparkEnv.get.blockManager.shuffleServerId, partitionLengths, mapId)
  }
  override def getRssPartitionWriter(
      handle: ShuffleHandle,
      mapId: Int,
      metrics: ShuffleWriteMetricsReporter,
      numPartitions: Int): Option[RssPartitionWriterBase] = None

  override def getMapStatus(
      shuffleServerId: BlockManagerId,
      partitionLengthMap: Array[Long],
      mapId: Long): MapStatus =
    MapStatus.apply(shuffleServerId, partitionLengthMap, mapId)

  override def getShuffleWriteExec(
      input: PhysicalPlanNode,
      nativeOutputPartitioning: PhysicalHashRepartition.Builder): PhysicalPlanNode = {
    PhysicalPlanNode
      .newBuilder()
      .setShuffleWriter(
        ShuffleWriterExecNode
          .newBuilder()
          .setInput(input)
          .setOutputPartitioning(nativeOutputPartitioning)
          .buildPartial()
      ) // shuffleId is not set at the moment, will be set in ShuffleWriteProcessor
      .build()
  }
}
