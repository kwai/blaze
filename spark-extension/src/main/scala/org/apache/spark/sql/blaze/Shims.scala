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
import org.apache.spark.SparkContext
import org.blaze.{protobuf => pb}
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
import org.apache.spark.sql.execution.blaze.plan._
import org.apache.spark.sql.execution.blaze.shuffle.RssPartitionWriterBase
import org.apache.spark.sql.execution.exchange.BroadcastExchangeLike
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Generator
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.blaze.plan.NativeBroadcastJoinBase
import org.apache.spark.sql.execution.blaze.plan.NativeSortMergeJoinBase
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.storage.FileSegment

abstract class Shims {

  def initExtension(): Unit = {}

  def onApplyingExtension(): Unit = {}

  def createConvertToNativeExec(child: SparkPlan): ConvertToNativeBase

  def createNativeAggExec(
      execMode: NativeAggBase.AggExecMode,
      requiredChildDistributionExpressions: Option[Seq[Expression]],
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      aggregateAttributes: Seq[Attribute],
      initialInputBufferOffset: Int,
      child: SparkPlan): NativeAggBase

  def createNativeBroadcastExchangeExec(
      mode: BroadcastMode,
      child: SparkPlan): NativeBroadcastExchangeBase

  def createNativeBroadcastJoinExec(
      left: SparkPlan,
      right: SparkPlan,
      outputPartitioning: Partitioning,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      broadcastSide: BroadcastSide): NativeBroadcastJoinBase

  def createNativeSortMergeJoinExec(
      left: SparkPlan,
      right: SparkPlan,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType): NativeSortMergeJoinBase

  def createNativeShuffledHashJoinExec(
      left: SparkPlan,
      right: SparkPlan,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      buildSide: BuildSide): SparkPlan

  def createNativeExpandExec(
      projections: Seq[Seq[Expression]],
      output: Seq[Attribute],
      child: SparkPlan): NativeExpandBase

  def createNativeFilterExec(condition: Expression, child: SparkPlan): NativeFilterBase

  def createNativeGenerateExec(
      generator: Generator,
      requiredChildOutput: Seq[Attribute],
      outer: Boolean,
      generatorOutput: Seq[Attribute],
      child: SparkPlan): NativeGenerateBase

  def createNativeGlobalLimitExec(limit: Long, child: SparkPlan): NativeGlobalLimitBase

  def createNativeLocalLimitExec(limit: Long, child: SparkPlan): NativeLocalLimitBase

  def createNativeParquetInsertIntoHiveTableExec(
      cmd: InsertIntoHiveTable,
      child: SparkPlan): NativeParquetInsertIntoHiveTableBase

  def createNativeParquetScanExec(basedFileScan: FileSourceScanExec): NativeParquetScanBase

  def createNativeProjectExec(
      projectList: Seq[NamedExpression],
      child: SparkPlan,
      addTypeCast: Boolean = false): NativeProjectBase

  def createNativeRenameColumnsExec(
      child: SparkPlan,
      newColumnNames: Seq[String]): NativeRenameColumnsBase

  def createNativeShuffleExchangeExec(
      outputPartitioning: Partitioning,
      child: SparkPlan): NativeShuffleExchangeBase

  def createNativeSortExec(
      sortOrder: Seq[SortOrder],
      global: Boolean,
      child: SparkPlan): NativeSortBase

  def createNativeTakeOrderedExec(
      limit: Long,
      sortOrder: Seq[SortOrder],
      child: SparkPlan): NativeTakeOrderedBase

  def createNativePartialTakeOrderedExec(
      limit: Long,
      sortOrder: Seq[SortOrder],
      child: SparkPlan,
      metrics: Map[String, SQLMetric]): NativePartialTakeOrderedBase

  def createNativeUnionExec(children: Seq[SparkPlan]): NativeUnionBase

  def createNativeWindowExec(
      windowExpression: Seq[NamedExpression],
      partitionSpec: Seq[Expression],
      orderSpec: Seq[SortOrder],
      child: SparkPlan): NativeWindowBase

  def createNativeParquetSinkExec(
      sparkSession: SparkSession,
      table: CatalogTable,
      partition: Map[String, Option[String]],
      child: SparkPlan,
      metrics: Map[String, SQLMetric]): NativeParquetSinkBase

  def isNative(plan: SparkPlan): Boolean

  def getUnderlyingNativePlan(plan: SparkPlan): NativeSupports

  def getUnderlyingBroadcast(plan: SparkPlan): BroadcastExchangeLike

  def executeNative(plan: SparkPlan): NativeRDD

  def isQueryStageInput(plan: SparkPlan): Boolean

  def isShuffleQueryStageInput(plan: SparkPlan): Boolean

  def getChildStage(plan: SparkPlan): SparkPlan

  def simpleStringWithNodeId(plan: SparkPlan): String

  def setLogicalLink(exec: SparkPlan, basedExec: SparkPlan): SparkPlan

  def getRDDShuffleReadFull(rdd: RDD[_]): Boolean

  def setRDDShuffleReadFull(rdd: RDD[_], shuffleReadFull: Boolean): Unit

  // shim methods for expressions

  def convertMoreExprWithFallback(
      e: Expression,
      isPruningExpr: Boolean,
      fallback: Expression => pb.PhysicalExprNode): Option[pb.PhysicalExprNode]

  def convertMoreAggregateExpr(e: AggregateExpression): Option[pb.PhysicalExprNode]

  def getLikeEscapeChar(expr: Expression): Char

  def getAggregateExpressionFilter(expr: Expression): Option[Expression]

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
      input: pb.PhysicalPlanNode,
      nativeOutputPartitioning: pb.PhysicalHashRepartition.Builder): pb.PhysicalPlanNode

  def convertMoreSparkPlan(exec: SparkPlan): Option[SparkPlan]

  def getSqlContext(sparkPlan: SparkPlan): SQLContext

  def createNativeExprWrapper(
      nativeExpr: pb.PhysicalExprNode,
      dataType: DataType,
      nullable: Boolean): Expression

  def postTransform(plan: SparkPlan, sc: SparkContext): Unit = {}
}

object Shims {
  lazy val get: Shims = {
    classOf[Shims].getClassLoader
      .loadClass("org.apache.spark.sql.blaze.ShimsImpl")
      .newInstance()
      .asInstanceOf[Shims]
  }
}
