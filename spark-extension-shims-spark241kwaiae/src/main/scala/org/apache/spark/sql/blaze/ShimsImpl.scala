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

import com.kuaishou.dataarch.shuffle.proto.dto.common.PartitionStatistics
import org.apache.commons.lang3.reflect.FieldUtils
import org.apache.commons.lang3.reflect.MethodUtils
import org.apache.spark.OneToOneDependency
import org.apache.spark.Partition
import org.apache.spark.ShuffleDependency
import org.apache.spark.SparkEnv
import org.apache.spark.SparkException
import org.apache.spark.TaskContext
import org.blaze.{protobuf => pb}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.OptionalConfigEntry
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.ShuffledRDDPartition
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.shuffle.ShuffleReader
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.shuffle.UnifiedShuffleManager
import org.apache.spark.shuffle.sort.MapInfo
import org.apache.spark.sql.blaze.kwai.BlazeOperatorMetricsCollector
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.First
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.StringSplit
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageInput
import org.apache.spark.sql.execution.adaptive.LocalShuffledRowRDD
import org.apache.spark.sql.execution.adaptive.QueryStage
import org.apache.spark.sql.execution.adaptive.QueryStageInput
import org.apache.spark.sql.execution.adaptive.ShuffleQueryStageInput
import org.apache.spark.sql.execution.adaptive.SkewedShuffleQueryStageInput
import org.apache.spark.sql.execution.blaze.plan.NativeParquetScanBase
import org.apache.spark.sql.execution.blaze.plan.NativeShuffleExchangeExec
import org.apache.spark.sql.execution.blaze.plan.NativeUnionExec
import org.apache.spark.sql.execution.blaze.shuffle.BlazeBlockStoreShuffleReaderBase
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.metric.SQLShuffleReadMetricsReporter
import org.apache.spark.sql.execution.blaze.plan.NativeBroadcastExchangeBase
import org.apache.spark.sql.execution.blaze.plan.NativeBroadcastExchangeExec
import org.apache.spark.sql.execution.datasources.BasicWriteTaskStats
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.blaze.plan.NativeParquetScanExec
import org.apache.spark.sql.execution.blaze.plan.NativeShuffleExchangeBase
import org.apache.spark.sql.execution.blaze.shuffle.RssPartitionWriter
import org.apache.spark.sql.execution.exchange.BroadcastExchangeLike
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.blaze.plan.NativeRenameColumnsExec
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.blaze.BlazeConverters.ForceNativeExecutionWrapperBase
import org.apache.spark.sql.blaze.NativeConverters.NativeExprWrapperBase
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Generator
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.blaze.plan.ConvertToNativeBase
import org.apache.spark.sql.execution.blaze.plan.ConvertToNativeExec
import org.apache.spark.sql.execution.blaze.plan.NativeAggBase
import org.apache.spark.sql.execution.blaze.plan.NativeAggBase.AggExecMode
import org.apache.spark.sql.execution.blaze.plan.NativeAggExec
import org.apache.spark.sql.execution.blaze.plan.NativeBroadcastJoinBase
import org.apache.spark.sql.execution.blaze.plan.NativeBroadcastJoinExec
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
import org.apache.spark.sql.execution.blaze.plan.NativePartialTakeOrderedBase
import org.apache.spark.sql.execution.blaze.plan.NativePartialTakeOrderedExec
import org.apache.spark.sql.execution.blaze.plan.NativeProjectBase
import org.apache.spark.sql.execution.blaze.plan.NativeProjectExec
import org.apache.spark.sql.execution.blaze.plan.NativeRenameColumnsBase
import org.apache.spark.sql.execution.blaze.plan.NativeSortBase
import org.apache.spark.sql.execution.blaze.plan.NativeSortExec
import org.apache.spark.sql.execution.blaze.plan.NativeSortMergeJoinBase
import org.apache.spark.sql.execution.blaze.plan.NativeSortMergeJoinExec
import org.apache.spark.sql.execution.blaze.plan.NativeTakeOrderedBase
import org.apache.spark.sql.execution.blaze.plan.NativeTakeOrderedExec
import org.apache.spark.sql.execution.blaze.plan.NativeUnionBase
import org.apache.spark.sql.execution.blaze.plan.NativeWindowBase
import org.apache.spark.sql.execution.blaze.plan.NativeWindowExec
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.blaze.plan.NativeBroadcastNestedLoopJoinBase
import org.apache.spark.sql.execution.blaze.plan.NativeBroadcastNestedLoopJoinExec
import org.apache.spark.sql.execution.blaze.plan.NativeParquetSinkBase
import org.apache.spark.sql.execution.blaze.plan.NativeParquetSinkExec
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.storage.FileSegment
import org.apache.spark.storage.ShuffleDataBlockId
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.ExternalBlockStoreUtils
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.util.Utils
import org.blaze.protobuf.PhysicalExprNode

class ShimsImpl extends Shims with Logging {

  override def onApplyingExtension(extension: SparkSessionExtensions): Unit = {
    extension.injectOptimizerRule(sparkSession => {
      BlazeRuleEngine(sparkSession)
    })
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
      condition: Option[Expression]): NativeBroadcastNestedLoopJoinBase = {
    NativeBroadcastNestedLoopJoinExec(left, right, joinType, condition)
  }

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

  override def createNativeParquetSinkExec(
      sparkSession: SparkSession,
      table: CatalogTable,
      partition: Map[String, Option[String]],
      child: SparkPlan,
      metrics: Map[String, SQLMetric]): NativeParquetSinkBase =
    NativeParquetSinkExec(sparkSession, table, partition, child, metrics)

  override def getUnderlyingBroadcast(exec: SparkPlan): BroadcastExchangeLike = {
    exec match {
      case exec: BroadcastExchangeLike => exec
      case exec: UnaryExecNode => getUnderlyingBroadcast(exec.child)
    }
  }

  override def getRDDShuffleReadFull(rdd: RDD[_]): Boolean = {
    rdd.shuffleReadFull
  }

  override def setRDDShuffleReadFull(rdd: RDD[_], shuffleReadFull: Boolean): Unit = {
    rdd.shuffleReadFull = shuffleReadFull
  }

  override def getRssPartitionWriter(
      handle: ShuffleHandle,
      mapId: Int,
      metrics: ShuffleWriteMetricsReporter,
      numPartitions: Int): Option[RssPartitionWriter] = {
    Some(new RssPartitionWriter(handle, mapId, metrics, numPartitions))
  }

  override def createFileSegment(
      file: File,
      offset: Long,
      length: Long,
      numRecords: Long): FileSegment = new FileSegment(file, offset, length, numRecords)

  override def commit(
      dep: ShuffleDependency[_, _, _],
      shuffleBlockResolver: IndexShuffleBlockResolver,
      tempDataFile: File,
      mapId: Long,
      partitionLengths: Array[Long],
      dataSize: Long,
      context: TaskContext): MapStatus = {

    // commit
    shuffleBlockResolver.writeIndexFileAndCommit(
      dep.shuffleId,
      mapId.toInt,
      partitionLengths,
      tempDataFile)

    // shuffle write on hdfs
    val blockManager = SparkEnv.get.blockManager
    val handle = dep.shuffleHandle.asInstanceOf[BaseShuffleHandle[_, _, _]]
    val output = shuffleBlockResolver.getDataFile(dep.shuffleId, mapId.toInt)
    val mapInfo = new MapInfo(partitionLengths, partitionLengths.map(_ => 0L))
    val totalPartitionLength = mapInfo.lengths.sum
    var hasExternalData = false

    if (ExternalBlockStoreUtils.writeRemoteEnabled(
        SparkEnv.get.conf,
        totalPartitionLength,
        handle.numMaps)) {
      if (dataSize != 0) {
        logInfo(s"KwaiShuffle: Start to write remote, shuffle block id ${ShuffleDataBlockId(
          dep.shuffleId,
          mapId.toInt,
          0).name}, file path is ${output.getPath}/${output.getName}")
        context.taskMetrics.externalMetrics.writeRemoteShuffle.setValue(1L)
        val dataBlockId = new ShuffleDataBlockId(
          dep.shuffleId,
          mapId.toInt,
          IndexShuffleBlockResolver.NOOP_REDUCE_ID)
        val indexFile = shuffleBlockResolver.getIndexFile(dep.shuffleId, mapId.toInt)
        blockManager.externalBlockStore.externalBlockManager.get.writeExternalShuffleFile(
          dep.shuffleId,
          mapId.toInt,
          dataBlockId,
          indexFile,
          output)
        hasExternalData = true
      }
    }

    val mapStatus =
      MapStatus.apply(SparkEnv.get.blockManager.shuffleServerId, partitionLengths)
    mapStatus.hasExternal = hasExternalData
    mapStatus
  }

  override def getMapStatus(
      shuffleServerId: BlockManagerId,
      partitionLengthMap: Array[Long],
      mapId: Long): MapStatus = {
    val mapStatus = MapStatus.apply(shuffleServerId, partitionLengthMap)
    mapStatus.hasExternal = false
    mapStatus
  }

  override def getShuffleWriteExec(
      input: pb.PhysicalPlanNode,
      nativeOutputPartitioning: pb.PhysicalHashRepartition.Builder): pb.PhysicalPlanNode = {
    if (SparkEnv.get.conf.get("spark.shuffle.manager", "sort").equals("unify")) {
      pb.PhysicalPlanNode
        .newBuilder()
        .setRssShuffleWriter(
          pb.RssShuffleWriterExecNode
            .newBuilder()
            .setInput(input)
            .setOutputPartitioning(nativeOutputPartitioning)
            .buildPartial()
        ) // shuffleId is not set at the moment, will be set in ShuffleWriteProcessor
        .build()
    } else {
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
  }

  override def isNative(plan: SparkPlan): Boolean = {
    plan match {
      case _: NativeSupports => true
      case plan: QueryStageInput => isNative(plan.childStage)
      case plan: QueryStage => isNative(plan.child)
      case plan: ReusedExchangeExec => isNative(plan.child)
      case _ => false
    }
  }

  override def getUnderlyingNativePlan(plan: SparkPlan): NativeSupports = {
    plan match {
      case plan: NativeSupports => plan
      case plan: QueryStageInput => getUnderlyingNativePlan(plan.childStage)
      case plan: QueryStage => getUnderlyingNativePlan(plan.child)
      case plan: ReusedExchangeExec => getUnderlyingNativePlan(plan.child)
      case _ => throw new RuntimeException("unreachable: plan is not native")
    }
  }

  override def executeNative(plan: SparkPlan): NativeRDD =
    plan match {
      case plan: NativeSupports =>
        BlazeOperatorMetricsCollector.instance.foreach(
          _.createListener(plan, plan.sqlContext.sparkContext))
        plan.executeNative()
      case plan: ShuffleQueryStageInput => executeNativeCustomShuffleReader(plan)
      case plan: SkewedShuffleQueryStageInput => executeNativeCustomShuffleReader(plan)
      case plan: BroadcastQueryStageInput => executeNative(plan.childStage)
      case plan: QueryStage => executeNative(plan.child)
      case plan: ReusedExchangeExec => executeNative(plan.child)
      case _ => throw new SparkException(s"Underlying plan is not NativeSupports: ${plan}")
    }

  override def isQueryStageInput(plan: SparkPlan): Boolean =
    plan.isInstanceOf[QueryStageInput]

  override def isShuffleQueryStageInput(plan: SparkPlan): Boolean = {
    plan.isInstanceOf[ShuffleQueryStageInput] || plan.isInstanceOf[SkewedShuffleQueryStageInput]
  }

  override def getChildStage(plan: SparkPlan): SparkPlan =
    plan.asInstanceOf[QueryStageInput].childStage

  override def setLogicalLink(exec: SparkPlan, basedExec: SparkPlan): SparkPlan =
    exec

  override def simpleStringWithNodeId(plan: SparkPlan): String = plan.simpleString

  override def convertExpr(e: Expression): Option[pb.PhysicalExprNode] = {
    e match {
      case StringSplit(str, pat @ Literal(_, StringType))
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

  override def getLikeEscapeChar(expr: Expression): Char = '\\'

  override def convertAggregateExpr(e: AggregateExpression): Option[pb.PhysicalExprNode] = {
    assert(getAggregateExpressionFilter(e).isEmpty)
    val aggBuilder = pb.PhysicalAggExprNode.newBuilder()

    e.aggregateFunction match {
      case First(child, ignoresNullExpr) =>
        aggBuilder.setAggFunction(if (ignoresNullExpr.eval().asInstanceOf[Boolean]) {
          pb.AggFunction.FIRST_IGNORES_NULL
        } else {
          pb.AggFunction.FIRST
        })
        aggBuilder.addChildren(NativeConverters.convertExpr(child))
        Some(pb.PhysicalExprNode.newBuilder().setAggExpr(aggBuilder).build())

      case _ => None
    }
  }

  override def getAggregateExpressionFilter(expr: Expression): Option[Expression] = None

  private def executeNativeCustomShuffleReader(exec: SparkPlan): NativeRDD = {
    exec match {
      case _: ShuffleQueryStageInput | _: SkewedShuffleQueryStageInput =>
        val shuffledRDD = exec.execute()
        val dependency = shuffledRDD.getClass
          .getMethod("dependency")
          .invoke(shuffledRDD)
          .asInstanceOf[ShuffleDependency[_, _, _]]
        val shuffleHandle = dependency.shuffleHandle

        val shuffleExec = exec
          .asInstanceOf[QueryStageInput]
          .childStage
          .child
          .asInstanceOf[NativeShuffleExchangeExec]
        val inputMetrics = shuffleExec.metrics
        val inputRDD = exec match {
          case exec: ShuffleQueryStageInput => executeNative(exec.childStage)
          case exec: SkewedShuffleQueryStageInput => executeNative(exec.childStage)
        }

        val nativeSchema = shuffleExec.nativeSchema
        val metrics = inputRDD.metrics
        val partitionClsName = shuffledRDD.getClass.getSimpleName
        val rebalancedPartitions = shuffledRDD match {
          case rdd: LocalShuffledRowRDD =>
            Some(
              FieldUtils
                .readDeclaredField(
                  rdd,
                  "org$apache$spark$sql$execution$adaptive$LocalShuffledRowRDD$$rebalancedPartitions",
                  true)
                .asInstanceOf[Array[_]])
          case _ =>
            None
        }

        new NativeRDD(
          shuffledRDD.sparkContext,
          metrics,
          shuffledRDD.partitions,
          new OneToOneDependency(shuffledRDD) :: Nil,
          shuffledRDD.shuffleReadFull,
          (partition, taskContext) => {
            val shuffleReadMetrics = taskContext.taskMetrics().createTempShuffleReadMetrics()
            val metricsReporter =
              new SQLShuffleReadMetricsReporter(shuffleReadMetrics, inputMetrics)

            val classOfShuffledRowRDDPartition =
              Utils.classForName("org.apache.spark.sql.execution.ShuffledRowRDDPartition")
            val classOfAdaptiveShuffledRowRDDPartition =
              Utils.classForName(
                "org.apache.spark.sql.execution.adaptive.AdaptiveShuffledRowRDDPartition")

            val readers: Iterator[ShuffleReader[_, _]] = shuffledRDD match {
              case rdd: LocalShuffledRowRDD =>
                ShimsImpl
                  .getLocalShuffleReaders(
                    taskContext,
                    rdd,
                    dependency,
                    rebalancedPartitions.get,
                    partition)
                  .toIterator
              case _ =>
                partition match {
                  case p if classOfShuffledRowRDDPartition.isInstance(p) =>
                    val clz = classOfShuffledRowRDDPartition
                    val startPreShufflePartitionIndex =
                      clz.getMethod("startPreShufflePartitionIndex").invoke(p).asInstanceOf[Int]
                    val endPreShufflePartitionIndex =
                      clz.getMethod("endPreShufflePartitionIndex").invoke(p).asInstanceOf[Int]

                    Iterator.single(
                      SparkEnv.get.shuffleManager
                        .getReader(
                          shuffleHandle,
                          startPreShufflePartitionIndex,
                          endPreShufflePartitionIndex,
                          taskContext,
                          metricsReporter))

                  case p if classOfAdaptiveShuffledRowRDDPartition.isInstance(p) =>
                    val clz = classOfAdaptiveShuffledRowRDDPartition
                    val preShufflePartitionIndex =
                      clz.getMethod("preShufflePartitionIndex").invoke(p).asInstanceOf[Int]
                    val startMapId = clz.getMethod("startMapId").invoke(p).asInstanceOf[Int]
                    val endMapId = clz.getMethod("endMapId").invoke(p).asInstanceOf[Int]

                    Iterator.single(
                      SparkEnv.get.shuffleManager
                        .getReader(
                          shuffleHandle,
                          preShufflePartitionIndex,
                          preShufflePartitionIndex + 1,
                          taskContext,
                          metricsReporter,
                          startMapId,
                          endMapId))
                  case p =>
                    Iterator.single(
                      SparkEnv.get.shuffleManager.getReader(
                        shuffleHandle,
                        p.index,
                        p.index + 1,
                        taskContext,
                        metricsReporter))
                }
            }

            // store fetch iterator in jni resource before native compute
            val jniResourceId = s"NativeShuffleReadExec:${UUID.randomUUID().toString}"
            JniBridge.resourcesMap.put(
              jniResourceId,
              () => {
                CompletionIterator[Object, Iterator[Object]](
                  readers.flatMap(
                    _.asInstanceOf[BlazeBlockStoreShuffleReaderBase[_, _]].readIpc()),
                  taskContext.taskMetrics().mergeShuffleReadMetrics(true))
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
          },
          friendlyName = s"NativeRDD.ShuffleRead [$partitionClsName]")
    }
  }

  override def convertMoreSparkPlan(exec: SparkPlan): Option[SparkPlan] = {
    exec match {
      case _: ShuffleQueryStageInput | _: SkewedShuffleQueryStageInput | _: ReusedExchangeExec
          if isNative(exec) =>
        case class ForceNativeExecutionWrapper(override val child: SparkPlan)
            extends ForceNativeExecutionWrapperBase(child) {

          override def withNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
            copy(child = newChildren.head)

          override protected def doCanonicalize(): SparkPlan = child.canonicalized
        }
        Some(ForceNativeExecutionWrapper(BlazeConverters.addRenameColumnsExec(exec)))

      case _ =>
        None
    }
  }

  override def getSqlContext(sparkPlan: SparkPlan): SQLContext = sparkPlan.sqlContext

  override def createNativeExprWrapper(
      nativeExpr: PhysicalExprNode,
      dataType: DataType,
      nullable: Boolean): Expression = {
    case class NativeExprWrapper(
        nativeExpr: PhysicalExprNode,
        override val dataType: DataType,
        override val nullable: Boolean)
        extends NativeExprWrapperBase(nativeExpr, dataType, nullable)
    NativeExprWrapper(nativeExpr, dataType, nullable)
  }
}

object ShimsImpl extends Logging {

  // copied from spark LocalShuffleRowRDD
  def getLocalShuffleReaders(
      context: TaskContext,
      rdd: LocalShuffledRowRDD,
      dependency: ShuffleDependency[_, _, _],
      rebalancedPartitions: Array[_],
      split: Partition): Iterable[ShuffleReader[_, _]] = {

    val shuffledRowPartition = split.asInstanceOf[ShuffledRDDPartition]
    val mapId = shuffledRowPartition.index
    val metrics = context.taskMetrics().createTempShuffleReadMetrics()

    if (rebalancedPartitions.nonEmpty) {
      val rebalancedPartition = rebalancedPartitions(mapId)
      val segments =
        MethodUtils.invokeMethod(rebalancedPartition, true, "segments").asInstanceOf[Seq[_]]
      val groupedRebalancedPartitions = segments.groupBy(segment =>
        MethodUtils.invokeMethod(segment, true, "partId").asInstanceOf[Int])

      groupedRebalancedPartitions.map { case (partId, partitionStatistics) =>
        SparkEnv.get.shuffleManager
          .asInstanceOf[UnifiedShuffleManager]
          .getRebalancedReader(
            dependency.shuffleHandle,
            partId,
            context,
            metrics,
            Seq({
              val fileInfo = new java.util.HashMap[String, java.lang.Long]()
              partitionStatistics.foreach { segment =>
                val fileName =
                  MethodUtils.invokeMethod(segment, true, "fileName").asInstanceOf[String]
                val partId = MethodUtils.invokeMethod(segment, true, "partId").asInstanceOf[Int]
                fileInfo.put(fileName, partId)
              }
              PartitionStatistics
                .builder()
                .stageId(dependency.shuffleId)
                .partId(partId)
                .fileInfo(fileInfo)
                .build()
            }))
      }

    } else {
      rdd.partitionStartIndices
        .zip(rdd.partitionEndIndices)
        .map { case (start, end) =>
          logInfo(s"Create local shuffle reader mapId $mapId, partition range $start-$end")
          SparkEnv.get.shuffleManager
            .getReader(dependency.shuffleHandle, start, end, context, metrics, mapId, mapId + 1)
        }
    }
  }
}

case class BlazeRuleEngine(sparkSession: SparkSession) extends Rule[LogicalPlan] with Logging {
  import BlazeSparkSessionExtension._

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.foreachUp {
      case p @ LogicalRelation(fsRelation: HadoopFsRelation, _, _, _) =>
        // non parquet table rule
        if (!fsRelation.fileFormat.isInstanceOf[ParquetFileFormat]) {
          turnOffBlazeWithReason(p.conf, BlazeMissPatterns.NonParquetFormat)
        }

        // read encrypted table rule
        val readEncryptedTableEnable = sparkSession.sparkContext.conf
          .getBoolean("spark.hive.exist.read.encrypted.table", defaultValue = false)
        if (readEncryptedTableEnable) {
          turnOffBlazeWithReason(p.conf, BlazeMissPatterns.ReadEncryptedTable)
        }

        // skip scan dp_dd.*** table because parquet statics don't get min/max info
        if (p.catalogTable.map(_.identifier.unquotedString).getOrElse("").contains("dp_dd")) {
          turnOffBlazeWithReason(p.conf, BlazeMissPatterns.ReadHbaseTable)
        }

      case h: HiveTableRelation =>
        turnOffBlazeWithReason(h.conf, BlazeMissPatterns.NonParquetFormat)

      case _ =>
    }
    plan
  }

  private def turnOffBlazeWithReason(planConf: SQLConf, blazeMissPattern: String): Unit = {
    planConf.setConf(blazeEnabledKey, false)
    sparkSession.sparkContext.conf
      .set(BlazeRuleEngine.blazeMissPatterns, blazeMissPattern)
  }

  object BlazeMissPatterns extends Enumeration {
    val NonParquetFormat = "NonParquetFormat"
    val ReadEncryptedTable = "ReadEncryptedTable"
    val ReadHbaseTable = "ReadHbaseTable"
  }
}

object BlazeRuleEngine {
  lazy val blazeMissPatterns: OptionalConfigEntry[String] = SQLConf
    .buildConf("spark.blaze.blazeMissPatterns")
    .stringConf
    .createOptional
}
