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
import java.util.UUID

import org.apache.spark.ShuffleDependency
import org.apache.spark.SparkEnv
import org.apache.spark.SparkException
import org.apache.spark.TaskContext
import org.blaze.{protobuf => pb}

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.First
import org.apache.spark.sql.catalyst.expressions.Like
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.StringSplit
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.datasources.BasicWriteTaskStats
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.CustomShuffleReaderExec
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.sql.execution.adaptive.ShuffleQueryStageExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.ShuffledRowRDD
import org.apache.spark.sql.execution.blaze.plan.NativeShuffleExchangeExec
import org.apache.spark.sql.execution.CoalescedPartitionSpec
import org.apache.spark.sql.execution.ShufflePartitionSpec
import org.apache.spark.sql.execution.blaze.shuffle.BlazeBlockStoreShuffleReader
import org.apache.spark.sql.execution.PartialMapperPartitionSpec
import org.apache.spark.sql.execution.PartialReducerPartitionSpec
import org.apache.spark.sql.execution.blaze.plan.NativeParquetScanBase
import org.apache.spark.sql.execution.blaze.plan.NativeParquetScanExec
import org.apache.spark.sql.execution.blaze.plan.NativeShuffleExchangeBase
import org.apache.spark.sql.execution.blaze.plan.NativeUnionExec
import org.apache.spark.sql.execution.blaze.shuffle.RssPartitionWriterBase
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.blaze.plan.NativeBroadcastExchangeBase
import org.apache.spark.sql.execution.blaze.plan.NativeBroadcastExchangeExec
import org.apache.spark.sql.execution.exchange.BroadcastExchangeLike
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.storage.FileSegment

class ShimsImpl extends Shims with Logging {

  override def getUnderlyingBroadcast(plan: SparkPlan): BroadcastExchangeLike = {
    plan match {
      case exec: BroadcastExchangeLike => exec
      case exec: UnaryExecNode => getUnderlyingBroadcast(exec.child)
      case exec: BroadcastQueryStageExec => getUnderlyingBroadcast(exec.broadcast)
    }
  }

  override def isNative(plan: SparkPlan): Boolean =
    plan match {
      case _: NativeSupports => true
      case plan: CustomShuffleReaderExec => isNative(plan.child)
      case plan: QueryStageExec => isNative(plan.plan)
      case plan: ReusedExchangeExec => isNative(plan.child)
      case _ => false
    }

  override def getUnderlyingNativePlan(plan: SparkPlan): NativeSupports = {
    plan match {
      case plan: NativeSupports => plan
      case plan: CustomShuffleReaderExec => getUnderlyingNativePlan(plan.child)
      case plan: QueryStageExec => getUnderlyingNativePlan(plan.plan)
      case plan: ReusedExchangeExec => getUnderlyingNativePlan(plan.child)
      case _ => throw new RuntimeException("unreachable: plan is not native")
    }
  }

  override def executeNative(plan: SparkPlan): NativeRDD = {
    plan match {
      case plan: NativeSupports =>
        val executeQueryMethod =
          classOf[SparkPlan].getDeclaredMethod("executeQuery", classOf[() => _])
        executeQueryMethod.setAccessible(true)
        executeQueryMethod.invoke(plan, () => plan.doExecuteNative()).asInstanceOf[NativeRDD]

      case plan: CustomShuffleReaderExec => executeNativeCustomShuffleReader(plan)
      case plan: QueryStageExec => executeNative(plan.plan)
      case plan: ReusedExchangeExec => executeNative(plan.child)
      case _ =>
        throw new SparkException(s"Underlying plan is not NativeSupports: ${plan}")
    }
  }

  override def isQueryStageInput(plan: SparkPlan): Boolean = {
    plan.isInstanceOf[QueryStageExec]
  }

  override def isShuffleQueryStageInput(plan: SparkPlan): Boolean = {
    plan.isInstanceOf[ShuffleQueryStageExec]
  }

  override def getChildStage(plan: SparkPlan): SparkPlan =
    plan.asInstanceOf[QueryStageExec].plan

  override def insertForceNativeExecutionWrapper(exec: SparkPlan): SparkPlan = {
    exec match {
      case _: ShuffleQueryStageExec | _: CustomShuffleReaderExec =>
        ForceNativeExecutionWrapper(exec)
      case other =>
        other.mapChildren(child => insertForceNativeExecutionWrapper(child))
    }
  }

  override def needRenameColumns(plan: SparkPlan): Boolean = {
    if (plan.output.isEmpty) {
      return false
    }
    // use shim to get isQueryStageInput and getChildStage
    plan match {
      case _: NativeParquetScanExec | _: NativeUnionExec | _: ReusedExchangeExec => true
      case exec: QueryStageExec =>
        needRenameColumns(getChildStage(exec)) || exec.output != getChildStage(exec).output
      case CustomShuffleReaderExec(child, _, _) => needRenameColumns(child)
      case ForceNativeExecutionWrapper(child) => needRenameColumns(child)
      case _ => false
    }
  }

  override def simpleStringWithNodeId(plan: SparkPlan): String = plan.simpleStringWithNodeId()

  override def setLogicalLink(exec: SparkPlan, basedExec: SparkPlan): SparkPlan = {
    basedExec.logicalLink.foreach(logicalLink => exec.setLogicalLink(logicalLink))
    exec
  }

  override def createBasicWriteTaskStats(params: Map[String, Any]): BasicWriteTaskStats = {
    BasicWriteTaskStats(
      params.get("numPartitions").map(_.asInstanceOf[Int]).getOrElse(0),
      params.get("numFiles").map(_.asInstanceOf[Int]).getOrElse(0),
      params.get("numBytes").map(_.asInstanceOf[Long]).getOrElse(0),
      params.get("numRows").map(_.asInstanceOf[Long]).getOrElse(0))
  }

  override def getRDDShuffleReadFull(rdd: RDD[_]): Boolean = true

  override def setRDDShuffleReadFull(rdd: RDD[_], shuffleReadFull: Boolean): Unit = {}

  override def createArrowShuffleExchange(
      outputPartitioning: Partitioning,
      child: SparkPlan): NativeShuffleExchangeBase = {
    NativeShuffleExchangeExec(outputPartitioning, child)
  }

  override def createParquetScan(exec: FileSourceScanExec): NativeParquetScanBase = {
    NativeParquetScanExec(exec)
  }

  override def createArrowBroadcastExchange(
      mode: BroadcastMode,
      child: SparkPlan): NativeBroadcastExchangeBase = {
    NativeBroadcastExchangeExec(mode, child)
  }

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
      input: pb.PhysicalPlanNode,
      nativeOutputPartitioning: pb.PhysicalHashRepartition.Builder): pb.PhysicalPlanNode = {
    pb.PhysicalPlanNode
      .newBuilder()
      .setShuffleWriter(
        pb.ShuffleWriterExecNode
          .newBuilder()
          .setInput(input)
          .setOutputPartitioning(nativeOutputPartitioning)
          .buildPartial()
      ) // shuffleId is not set at the moment, will be set in ShuffleWriteProcessor
      .build()
  }

  override def convertExpr(e: Expression): Option[pb.PhysicalExprNode] = {
    e match {
      case StringSplit(str, pat @ Literal(_, StringType), Literal(-1, IntegerType))
          // native StringSplit implementation does not support regex, so only most frequently
          // used cases without regex are supported
          if Seq(",", ", ", ":", ";", "#", "@", "_", "-", "\\|", "\\.").contains(pat.value) =>
        val nativePat = pat.value match {
          case "\\|" => "|"
          case "\\." => "."
          case other => other
        }
        Some(
          pb.PhysicalExprNode
            .newBuilder()
            .setScalarFunction(
              pb.PhysicalScalarFunctionNode
                .newBuilder()
                .setFun(pb.ScalarFunction.SparkExtFunctions)
                .setName("StringSplit")
                .addArgs(NativeConverters.convertExpr(str))
                .addArgs(NativeConverters.convertExpr(Literal(nativePat)))
                .setReturnType(NativeConverters.convertDataType(StringType)))
            .build())

      case _ => None
    }
  }

  override def getLikeEscapeChar(expr: Expression): Char = {
    expr.asInstanceOf[Like].escapeChar
  }

  override def convertAggregateExpr(e: AggregateExpression): Option[pb.PhysicalExprNode] = {
    assert(getAggregateExpressionFilter(e).isEmpty)
    val aggBuilder = pb.PhysicalAggExprNode.newBuilder()

    e.aggregateFunction match {
      case First(child, ignoresNull) =>
        aggBuilder.setAggFunction(if (ignoresNull) {
          pb.AggFunction.FIRST_IGNORES_NULL
        } else {
          pb.AggFunction.FIRST
        })
        aggBuilder.addChildren(NativeConverters.convertExpr(child))
        Some(pb.PhysicalExprNode.newBuilder().setAggExpr(aggBuilder).build())

      case _ => None
    }
  }

  override def getAggregateExpressionFilter(expr: Expression): Option[Expression] = {
    expr.asInstanceOf[AggregateExpression].filter
  }

  private def executeNativeCustomShuffleReader(exec: CustomShuffleReaderExec): NativeRDD = {
    exec match {
      case CustomShuffleReaderExec(child, _, _) if isNative(child) =>
        val inputShuffledRowRDD = exec.execute().asInstanceOf[ShuffledRowRDD]
        val shuffleHandle = inputShuffledRowRDD.dependency.shuffleHandle

        val inputRDD = executeNative(child)
        val nativeSchema: pb.Schema = getUnderlyingNativePlan(child)
          .asInstanceOf[NativeShuffleExchangeExec]
          .nativeSchema
        val metrics = MetricNode(Map(), inputRDD.metrics :: Nil)

        new NativeRDD(
          inputShuffledRowRDD.sparkContext,
          metrics,
          inputShuffledRowRDD.partitions,
          inputShuffledRowRDD.dependencies,
          true,
          (partition, taskContext) => {

            // use reflection to get partitionSpec because ShuffledRowRDDPartition is private
            // scalastyle:off classforname
            val shuffledRDDPartitionClass =
              Class.forName("org.apache.spark.sql.execution.ShuffledRowRDDPartition")
            // scalastyle:on classforname
            val specField = shuffledRDDPartitionClass.getDeclaredField("spec")
            specField.setAccessible(true)
            val sqlMetricsReporter = taskContext.taskMetrics().createTempShuffleReadMetrics()
            val spec = specField.get(partition).asInstanceOf[ShufflePartitionSpec]
            val reader = spec match {
              case CoalescedPartitionSpec(startReducerIndex, endReducerIndex) =>
                SparkEnv.get.shuffleManager
                  .getReader(
                    shuffleHandle,
                    startReducerIndex,
                    endReducerIndex,
                    taskContext,
                    sqlMetricsReporter)
                  .asInstanceOf[BlazeBlockStoreShuffleReader[_, _]]

              case PartialReducerPartitionSpec(reducerIndex, startMapIndex, endMapIndex) =>
                SparkEnv.get.shuffleManager
                  .getReaderForRange(
                    shuffleHandle,
                    startMapIndex,
                    endMapIndex,
                    reducerIndex,
                    reducerIndex + 1,
                    taskContext,
                    sqlMetricsReporter)
                  .asInstanceOf[BlazeBlockStoreShuffleReader[_, _]]

              case PartialMapperPartitionSpec(mapIndex, startReducerIndex, endReducerIndex) =>
                SparkEnv.get.shuffleManager
                  .getReaderForRange(
                    shuffleHandle,
                    mapIndex,
                    mapIndex + 1,
                    startReducerIndex,
                    endReducerIndex,
                    taskContext,
                    sqlMetricsReporter)
                  .asInstanceOf[BlazeBlockStoreShuffleReader[_, _]]
            }

            // store fetch iterator in jni resource before native compute
            val jniResourceId = s"NativeShuffleReadExec:${UUID.randomUUID().toString}"
            JniBridge.resourcesMap.put(jniResourceId, () => reader.readIpc())

            pb.PhysicalPlanNode
              .newBuilder()
              .setIpcReader(
                pb.IpcReaderExecNode
                  .newBuilder()
                  .setSchema(nativeSchema)
                  .setNumPartitions(inputShuffledRowRDD.getNumPartitions)
                  .setIpcProviderResourceId(jniResourceId)
                  .setMode(pb.IpcReadMode.CHANNEL_AND_FILE_SEGMENT)
                  .build())
              .build()
          })
    }
  }
}
