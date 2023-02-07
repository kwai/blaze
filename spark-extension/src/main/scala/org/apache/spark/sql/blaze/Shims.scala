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
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{
  IndexShuffleBlockResolver,
  ShuffleHandle,
  ShuffleManager,
  ShuffleWriteMetricsReporter
}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.blaze.plan.NativeBroadcastExchangeBase
import org.apache.spark.sql.execution.blaze.plan.NativeShuffleExchangeBase
import org.apache.spark.sql.execution.blaze.plan.NativeParquetScanBase
import org.apache.spark.sql.execution.blaze.shuffle.RssPartitionWriterBase
import org.apache.spark.storage.{BlockManagerId, FileSegment}
import org.blaze.protobuf.{PhysicalHashRepartition, PhysicalPlanNode}

abstract class Shims {
  def rddShims: RDDShims
  def sparkPlanShims: SparkPlanShims
  def shuffleShims: ShuffleShims
  def broadcastShims: BroadcastShims
  def exprShims: ExprShims
  def parquetScanShims: ParquetScanShims
}
object Shims {
  lazy val get: Shims = {
    classOf[Shims].getClassLoader
      .loadClass("org.apache.spark.sql.blaze.ShimsImpl")
      .newInstance()
      .asInstanceOf[Shims]
  }
}

trait RDDShims {
  def getShuffleReadFull(rdd: RDD[_]): Boolean
  def setShuffleReadFull(rdd: RDD[_], shuffleReadFull: Boolean): Unit
}

trait SparkPlanShims {
  def isNative(plan: SparkPlan): Boolean
  def getUnderlyingNativePlan(plan: SparkPlan): NativeSupports
  def executeNative(plan: SparkPlan): NativeRDD

  def isQueryStageInput(plan: SparkPlan): Boolean
  def isShuffleQueryStageInput(plan: SparkPlan): Boolean
  def getChildStage(plan: SparkPlan): SparkPlan
  def needRenameColumns(plan: SparkPlan): Boolean
  def setLogicalLink(exec: SparkPlan, basedExec: SparkPlan): SparkPlan
  def simpleStringWithNodeId(plan: SparkPlan): String
}

trait ShuffleShims {
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
      PartitionLengthMap: Array[Long],
      mapId: Long): MapStatus

  def getShuffleWriteExec(
      input: PhysicalPlanNode,
      nativeOutputPartitioning: PhysicalHashRepartition.Builder): PhysicalPlanNode
}

trait ParquetScanShims {
  def createParquetScan(exec: FileSourceScanExec): NativeParquetScanBase
}

trait BroadcastShims {
  def createArrowBroadcastExchange(
      mode: BroadcastMode,
      child: SparkPlan): NativeBroadcastExchangeBase
}

trait ExprShims {
  def getEscapeChar(expr: Expression): Char
  def getAggregateExpressionFilter(expr: Expression): Option[Expression]
}
