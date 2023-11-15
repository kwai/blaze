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
import org.apache.commons.lang3.reflect.FieldUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.ShuffleDependency
import org.apache.spark.SparkEnv
import org.apache.spark.SparkException
import org.apache.spark.TaskContext
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
import org.apache.spark.OneToOneDependency
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.blaze.BlazeConverters.ForceNativeExecutionWrapperBase
import org.apache.spark.sql.blaze.NativeConverters.NativeExprWrapperBase
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Generator
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.adaptive.AQEShuffleReadExec
import org.apache.spark.sql.execution.blaze.plan._
import org.apache.spark.sql.execution.blaze.plan.ConvertToNativeExec
import org.apache.spark.sql.execution.blaze.plan.NativeAggBase
import org.apache.spark.sql.execution.blaze.plan.NativeAggBase.AggExecMode
import org.apache.spark.sql.execution.blaze.plan.NativeAggExec
import org.apache.spark.sql.execution.blaze.plan.NativeExpandBase
import org.apache.spark.sql.execution.blaze.plan.NativeExpandExec
import org.apache.spark.sql.execution.blaze.plan.NativeFilterBase
import org.apache.spark.sql.execution.blaze.plan.NativeFilterExec
import org.apache.spark.sql.execution.blaze.plan.NativeGenerateBase
import org.apache.spark.sql.execution.blaze.plan.NativeGenerateExec
import org.apache.spark.sql.execution.blaze.plan.NativeGlobalLimitBase
import org.apache.spark.sql.execution.blaze.plan.NativeGlobalLimitExec
import org.apache.spark.sql.execution.blaze.plan.NativeLocalLimitBase
import org.apache.spark.sql.execution.blaze.plan.NativeLocalLimitExec
import org.apache.spark.sql.execution.blaze.plan.NativeParquetInsertIntoHiveTableBase
import org.apache.spark.sql.execution.blaze.plan.NativeParquetInsertIntoHiveTableExec
import org.apache.spark.sql.execution.blaze.plan.NativeProjectBase
import org.apache.spark.sql.execution.blaze.plan.NativeProjectExec
import org.apache.spark.sql.execution.blaze.plan.NativeRenameColumnsBase
import org.apache.spark.sql.execution.blaze.plan.NativeRenameColumnsExec
import org.apache.spark.sql.execution.blaze.plan.NativeSortBase
import org.apache.spark.sql.execution.blaze.plan.NativeSortExec
import org.apache.spark.sql.execution.blaze.plan.NativeTakeOrderedBase
import org.apache.spark.sql.execution.blaze.plan.NativeTakeOrderedExec
import org.apache.spark.sql.execution.blaze.plan.NativeUnionBase
import org.apache.spark.sql.execution.blaze.plan.NativeUnionExec
import org.apache.spark.sql.execution.blaze.plan.NativeWindowBase
import org.apache.spark.sql.execution.blaze.plan.NativeWindowExec
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.CoalescedMapperPartitionSpec
import org.apache.spark.sql.execution.blaze.plan.Helper.getTaskResourceId
import org.apache.spark.sql.execution.datasources.BasicWriteJobStatsTracker
import org.apache.spark.sql.execution.datasources.BasicWriteTaskStatsTracker
import org.apache.spark.sql.execution.datasources.WriteTaskStats
import org.apache.spark.sql.execution.datasources.WriteTaskStatsTracker
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.joins.blaze.plan.NativeBroadcastJoinExec
import org.apache.spark.sql.execution.joins.blaze.plan.NativeBroadcastNestedLoopJoinExec
import org.apache.spark.sql.execution.joins.blaze.plan.NativeSortMergeJoinExec
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.SerializableConfiguration
import org.blaze.{protobuf => pb}

class ShimsImpl extends Shims with Logging {

  override def initExtension(): Unit = {
    ValidateSparkPlanInjector.inject()
  }

  override def createConvertToNativeExec(child: SparkPlan): ConvertToNativeBase =
    ConvertToNativeExec(child)

  override def createNativeAggExec(
      execMode: AggExecMode,
      requiredChildDistributionExpressions: Option[Seq[Expression]],
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      aggregateAttributes: Seq[Attribute],
      initialInputBufferOffset: Int,
      child: SparkPlan): NativeAggBase =
    NativeAggExec(
      execMode,
      requiredChildDistributionExpressions,
      groupingExpressions,
      aggregateExpressions,
      aggregateAttributes,
      initialInputBufferOffset,
      child)

  override def createNativeBroadcastExchangeExec(
      mode: BroadcastMode,
      child: SparkPlan): NativeBroadcastExchangeBase =
    NativeBroadcastExchangeExec(mode, child)

  override def createNativeBroadcastJoinExec(
      left: SparkPlan,
      right: SparkPlan,
      outputPartitioning: Partitioning,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      condition: Option[Expression]): NativeBroadcastJoinBase =
    NativeBroadcastJoinExec(
      left,
      right,
      outputPartitioning,
      leftKeys,
      rightKeys,
      joinType,
      condition)

  override def createNativeBroadcastNestedLoopJoinExec(
      left: SparkPlan,
      right: SparkPlan,
      joinType: JoinType,
      condition: Option[Expression]): NativeBroadcastNestedLoopJoinBase =
    NativeBroadcastNestedLoopJoinExec(left, right, joinType, condition)

  override def createNativeSortMergeJoinExec(
      left: SparkPlan,
      right: SparkPlan,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      condition: Option[Expression]): NativeSortMergeJoinBase =
    NativeSortMergeJoinExec(left, right, leftKeys, rightKeys, joinType, condition)

  override def createNativeExpandExec(
      projections: Seq[Seq[Expression]],
      output: Seq[Attribute],
      child: SparkPlan): NativeExpandBase =
    NativeExpandExec(projections, output, child)

  override def createNativeFilterExec(condition: Expression, child: SparkPlan): NativeFilterBase =
    NativeFilterExec(condition, child)

  override def createNativeGenerateExec(
      generator: Generator,
      requiredChildOutput: Seq[Attribute],
      outer: Boolean,
      generatorOutput: Seq[Attribute],
      child: SparkPlan): NativeGenerateBase =
    NativeGenerateExec(generator, requiredChildOutput, outer, generatorOutput, child)

  override def createNativeGlobalLimitExec(limit: Long, child: SparkPlan): NativeGlobalLimitBase =
    NativeGlobalLimitExec(limit, child)

  override def createNativeLocalLimitExec(limit: Long, child: SparkPlan): NativeLocalLimitBase =
    NativeLocalLimitExec(limit, child)

  override def createNativeParquetInsertIntoHiveTableExec(
      cmd: InsertIntoHiveTable,
      child: SparkPlan): NativeParquetInsertIntoHiveTableBase =
    NativeParquetInsertIntoHiveTableExec(cmd, child)

  override def createNativeParquetScanExec(
      basedFileScan: FileSourceScanExec): NativeParquetScanBase =
    NativeParquetScanExec(basedFileScan)

  override def createNativeIcebergScanExec(baseBatchExec: BatchScanExec): NativeIcebergScanBase =
    NativeIcebergScanExec(baseBatchExec)

  override def createNativeProjectExec(
      projectList: Seq[NamedExpression],
      child: SparkPlan,
      addTypeCast: Boolean = false): NativeProjectBase =
    NativeProjectExec(projectList, child, addTypeCast)

  override def createNativeRenameColumnsExec(
      child: SparkPlan,
      newColumnNames: Seq[String]): NativeRenameColumnsBase =
    NativeRenameColumnsExec(child, newColumnNames)

  override def createNativeShuffleExchangeExec(
      outputPartitioning: Partitioning,
      child: SparkPlan): NativeShuffleExchangeBase =
    NativeShuffleExchangeExec(outputPartitioning, child)

  override def createNativeSortExec(
      sortOrder: Seq[SortOrder],
      global: Boolean,
      child: SparkPlan): NativeSortBase =
    NativeSortExec(sortOrder, global, child)

  override def createNativeTakeOrderedExec(
      limit: Long,
      sortOrder: Seq[SortOrder],
      child: SparkPlan): NativeTakeOrderedBase =
    NativeTakeOrderedExec(limit, sortOrder, child)

  override def createNativePartialTakeOrderedExec(
      limit: Long,
      sortOrder: Seq[SortOrder],
      child: SparkPlan,
      metrics: Map[String, SQLMetric]): NativePartialTakeOrderedBase =
    NativePartialTakeOrderedExec(limit, sortOrder, child, metrics)

  override def createNativeUnionExec(children: Seq[SparkPlan]): NativeUnionBase =
    NativeUnionExec(children)

  override def createNativeWindowExec(
      windowExpression: Seq[NamedExpression],
      partitionSpec: Seq[Expression],
      orderSpec: Seq[SortOrder],
      child: SparkPlan): NativeWindowBase =
    NativeWindowExec(windowExpression, partitionSpec, orderSpec, child)

  override def getUnderlyingBroadcast(plan: SparkPlan): BroadcastExchangeLike = {
    plan match {
      case exec: BroadcastExchangeLike => exec
      case exec: UnaryExecNode => getUnderlyingBroadcast(exec.child)
      case exec: BroadcastQueryStageExec => getUnderlyingBroadcast(exec.broadcast)
      case exec: ReusedExchangeExec => getUnderlyingBroadcast(exec.child)
    }
  }

  override def isNative(plan: SparkPlan): Boolean =
    plan match {
      case _: NativeSupports => true
      case plan: AQEShuffleReadExec => isNative(plan.child)
      case plan: QueryStageExec => isNative(plan.plan)
      case plan: ReusedExchangeExec => isNative(plan.child)
      case _ => false
    }

  override def getUnderlyingNativePlan(plan: SparkPlan): NativeSupports = {
    plan match {
      case plan: NativeSupports => plan
      case plan: AQEShuffleReadExec => getUnderlyingNativePlan(plan.child)
      case plan: QueryStageExec => getUnderlyingNativePlan(plan.plan)
      case plan: ReusedExchangeExec => getUnderlyingNativePlan(plan.child)
      case _ => throw new RuntimeException("unreachable: plan is not native")
    }
  }

  override def executeNative(plan: SparkPlan): NativeRDD = {
    plan match {
      case plan: NativeSupports => plan.executeNative()
      case plan: AQEShuffleReadExec => executeNativeAQEShuffleReader(plan)
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

  override def simpleStringWithNodeId(plan: SparkPlan): String = plan.simpleStringWithNodeId()

  override def setLogicalLink(exec: SparkPlan, basedExec: SparkPlan): SparkPlan = {
    basedExec.logicalLink.foreach(logicalLink => exec.setLogicalLink(logicalLink))
    exec
  }

  override def createBasicWriteTaskStats(params: Map[String, Any]): BasicWriteTaskStats = {
    BasicWriteTaskStats(
      Nil, // FIXME: use real partition values
      params.get("numFiles").map(_.asInstanceOf[Int]).getOrElse(0),
      params.get("numBytes").map(_.asInstanceOf[Long]).getOrElse(0),
      params.get("numRows").map(_.asInstanceOf[Long]).getOrElse(0))
  }

  override def getRDDShuffleReadFull(rdd: RDD[_]): Boolean = true

  override def setRDDShuffleReadFull(rdd: RDD[_], shuffleReadFull: Boolean): Unit = {}

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

    val checksums = Array[Long]()
    shuffleBlockResolver.writeMetadataFileAndCommit(
      dep.shuffleId,
      mapId,
      partitionLengths,
      checksums,
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

  private def executeNativeAQEShuffleReader(exec: AQEShuffleReadExec): NativeRDD = {
    exec match {
      case AQEShuffleReadExec(child, _) if isNative(child) =>
        val shuffledRDD = exec.execute().asInstanceOf[ShuffledRowRDD]
        val shuffleHandle = shuffledRDD.dependency.shuffleHandle

        val inputRDD = executeNative(child)
        val nativeShuffle = getUnderlyingNativePlan(child).asInstanceOf[NativeShuffleExchangeExec]
        val nativeSchema: pb.Schema = nativeShuffle.nativeSchema
        val metrics = MetricNode(Map(), inputRDD.metrics :: Nil)

        new NativeRDD(
          shuffledRDD.sparkContext,
          metrics,
          shuffledRDD.partitions,
          new OneToOneDependency(shuffledRDD) :: Nil,
          true,
          (partition, taskContext) => {

            // use reflection to get partitionSpec because ShuffledRowRDDPartition is private
            val sqlMetricsReporter = taskContext.taskMetrics().createTempShuffleReadMetrics()
            val spec = FieldUtils
              .readDeclaredField(partition, "spec", true)
              .asInstanceOf[ShufflePartitionSpec]
            val reader = spec match {
              case CoalescedPartitionSpec(startReducerIndex, endReducerIndex, _) =>
                SparkEnv.get.shuffleManager.getReader(
                  shuffleHandle,
                  startReducerIndex,
                  endReducerIndex,
                  taskContext,
                  sqlMetricsReporter)

              case CoalescedMapperPartitionSpec(startMapIndex, endMapIndex, numReducers) =>
                SparkEnv.get.shuffleManager.getReader(
                  shuffleHandle,
                  startMapIndex,
                  endMapIndex,
                  0,
                  numReducers,
                  taskContext,
                  sqlMetricsReporter)

              case PartialReducerPartitionSpec(reducerIndex, startMapIndex, endMapIndex, _) =>
                SparkEnv.get.shuffleManager.getReader(
                  shuffleHandle,
                  startMapIndex,
                  endMapIndex,
                  reducerIndex,
                  reducerIndex + 1,
                  taskContext,
                  sqlMetricsReporter)

              case PartialMapperPartitionSpec(mapIndex, startReducerIndex, endReducerIndex) =>
                SparkEnv.get.shuffleManager.getReader(
                  shuffleHandle,
                  mapIndex,
                  mapIndex + 1,
                  startReducerIndex,
                  endReducerIndex,
                  taskContext,
                  sqlMetricsReporter)
            }

            // store fetch iterator in jni resource before native compute
            val jniResourceId = s"NativeShuffleReadExec:${UUID.randomUUID().toString}"
            JniBridge.resourcesMap.put(
              jniResourceId,
              () => {
                reader.asInstanceOf[BlazeBlockStoreShuffleReader[_, _]].readIpc()
              })

            pb.PhysicalPlanNode
              .newBuilder()
              .setIpcReader(
                pb.IpcReaderExecNode
                  .newBuilder()
                  .setSchema(nativeSchema)
                  .setNumPartitions(shuffledRDD.getNumPartitions)
                  .setIpcProviderResourceId(jniResourceId)
                  .setMode(pb.IpcReadMode.CHANNEL_AND_FILE_SEGMENT)
                  .build())
              .build()
          })
    }
  }

  override def convertMoreSparkPlan(exec: SparkPlan): Option[SparkPlan] = {
    exec match {
      case _: AQEShuffleReadExec | _: ReusedExchangeExec if isNative(exec) =>
        Some(ForceNativeExecutionWrapper(BlazeConverters.addRenameColumnsExec(exec)))

      case _ => None
    }
  }

  override def getSqlContext(sparkPlan: SparkPlan): SQLContext =
    sparkPlan.session.sqlContext

  override def createBasicWriteJobStatsTrackerForNativeParquetSink(
      serializableHadoopConf: SerializableConfiguration,
      metrics: Map[String, SQLMetric]): BasicWriteJobStatsTracker = {

    new BasicWriteJobStatsTracker(serializableHadoopConf, metrics) {
      // read task stats from resources, the value should be set
      // in BlazeParquetRecordWriter.close()
      override def newTaskInstance(): WriteTaskStatsTracker = {
        class StatsTracker(hadoopConf: Configuration)
            extends BasicWriteTaskStatsTracker(hadoopConf) {
          override def getFinalStats(taskCommitTime: Long): WriteTaskStats = {
            JniBridge.getResource(getTaskResourceId("taskStats")).asInstanceOf[WriteTaskStats]
          }
        }
        new StatsTracker(serializableHadoopConf.value)
      }
    }
  }

  override def createNativeExprWrapper(
      nativeExpr: pb.PhysicalExprNode,
      dataType: DataType,
      nullable: Boolean): Expression = {
    NativeExprWrapper(nativeExpr, dataType, nullable)
  }
}

case class ForceNativeExecutionWrapper(override val child: SparkPlan)
    extends ForceNativeExecutionWrapperBase(child) {
  override def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

case class NativeExprWrapper(
    nativeExpr: pb.PhysicalExprNode,
    override val dataType: DataType,
    override val nullable: Boolean)
    extends NativeExprWrapperBase(nativeExpr, dataType, nullable) {
  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy()
}
