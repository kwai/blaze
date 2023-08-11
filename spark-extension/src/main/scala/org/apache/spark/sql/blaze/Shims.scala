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
package org.apache.spark.sql.blaze

import java.io.File

import org.apache.spark.ShuffleDependency
import org.apache.spark.TaskContext
import org.blaze.{protobuf => pb}
import org.blaze.protobuf.PhysicalHashRepartition
import org.blaze.protobuf.PhysicalPlanNode

import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.blaze.plan.NativeBroadcastExchangeBase
import org.apache.spark.sql.execution.blaze.plan.NativeParquetScanBase
import org.apache.spark.sql.execution.blaze.plan.NativeShuffleExchangeBase
import org.apache.spark.sql.execution.blaze.shuffle.RssPartitionWriterBase
import org.apache.spark.sql.execution.datasources.BasicWriteTaskStats
import org.apache.spark.sql.execution.exchange.BroadcastExchangeLike
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.storage.FileSegment

abstract class Shims {

  // shim methods for execution

  def isNative(plan: SparkPlan): Boolean

  def getUnderlyingNativePlan(plan: SparkPlan): NativeSupports

  def getUnderlyingBroadcast(plan: SparkPlan): BroadcastExchangeLike

  def executeNative(plan: SparkPlan): NativeRDD

  def isQueryStageInput(plan: SparkPlan): Boolean

  def isShuffleQueryStageInput(plan: SparkPlan): Boolean

  def getChildStage(plan: SparkPlan): SparkPlan

  def needRenameColumns(plan: SparkPlan): Boolean

  def simpleStringWithNodeId(plan: SparkPlan): String

  def setLogicalLink(exec: SparkPlan, basedExec: SparkPlan): SparkPlan

  def createBasicWriteTaskStats(params: Map[String, Any]): BasicWriteTaskStats

  def getRDDShuffleReadFull(rdd: RDD[_]): Boolean

  def setRDDShuffleReadFull(rdd: RDD[_], shuffleReadFull: Boolean): Unit

  // shim methods for expressions

  def convertExpr(e: Expression): Option[pb.PhysicalExprNode]

  def convertAggregateExpr(e: AggregateExpression): Option[pb.PhysicalExprNode]

  def getLikeEscapeChar(expr: Expression): Char

  def getAggregateExpressionFilter(expr: Expression): Option[Expression]

  def createArrowShuffleExchange(
      outputPartitioning: Partitioning,
      child: SparkPlan): NativeShuffleExchangeBase

  def createFileSegment(file: File, offset: Long, length: Long, numRecords: Long): FileSegment

  def commit(
      dep: ShuffleDependency[_, _, _],
      shuffleBlockResolver: IndexShuffleBlockResolver,
      tempDataFile: File,
      mapId: Long,
      partitionLengths: Array[Long],
      dataSize: Long,
      context: TaskContext): MapStatus

  def getRssPartitionWriter(
      handle: ShuffleHandle,
      mapId: Int,
      metrics: ShuffleWriteMetricsReporter,
      numPartitions: Int): Option[RssPartitionWriterBase]

  def getMapStatus(
      shuffleServerId: BlockManagerId,
      partitionLengthMap: Array[Long],
      mapId: Long): MapStatus

  def getShuffleWriteExec(
      input: PhysicalPlanNode,
      nativeOutputPartitioning: PhysicalHashRepartition.Builder): PhysicalPlanNode

  def createParquetScan(exec: FileSourceScanExec): NativeParquetScanBase

  def createArrowBroadcastExchange(
      mode: BroadcastMode,
      child: SparkPlan): NativeBroadcastExchangeBase
}

object Shims {
  lazy val get: Shims = {
    classOf[Shims].getClassLoader
      .loadClass("org.apache.spark.sql.blaze.ShimsImpl")
      .newInstance()
      .asInstanceOf[Shims]
  }
}
