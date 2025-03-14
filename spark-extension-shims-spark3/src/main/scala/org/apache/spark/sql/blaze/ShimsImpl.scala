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
import org.apache.spark.OneToOneDependency
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
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.blaze.BlazeConverters.ForceNativeExecutionWrapperBase
import org.apache.spark.sql.blaze.NativeConverters.NativeExprWrapperBase
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Generator
import org.apache.spark.sql.catalyst.expressions.Like
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.expressions.StringSplit
import org.apache.spark.sql.catalyst.expressions.TaggingExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.expressions.aggregate.First
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.CoalescedPartitionSpec
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.PartialMapperPartitionSpec
import org.apache.spark.sql.execution.PartialReducerPartitionSpec
import org.apache.spark.sql.execution.ShufflePartitionSpec
import org.apache.spark.sql.execution.ShuffledRowRDD
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.sql.execution.adaptive.ShuffleQueryStageExec
import org.apache.spark.sql.execution.blaze.plan.ConvertToNativeExec
import org.apache.spark.sql.execution.blaze.plan.NativeAggBase
import org.apache.spark.sql.execution.blaze.plan.NativeAggBase.AggExecMode
import org.apache.spark.sql.execution.blaze.plan.NativeAggExec
import org.apache.spark.sql.execution.blaze.plan.NativeBroadcastExchangeBase
import org.apache.spark.sql.execution.blaze.plan.NativeBroadcastExchangeExec
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
import org.apache.spark.sql.execution.blaze.plan.NativeOrcScanExec
import org.apache.spark.sql.execution.blaze.plan.NativeParquetInsertIntoHiveTableBase
import org.apache.spark.sql.execution.blaze.plan.NativeParquetInsertIntoHiveTableExec
import org.apache.spark.sql.execution.blaze.plan.NativeParquetScanBase
import org.apache.spark.sql.execution.blaze.plan.NativeParquetScanExec
import org.apache.spark.sql.execution.blaze.plan.NativeProjectBase
import org.apache.spark.sql.execution.blaze.plan.NativeRenameColumnsBase
import org.apache.spark.sql.execution.blaze.plan.NativeShuffleExchangeBase
import org.apache.spark.sql.execution.blaze.plan.NativeShuffleExchangeExec
import org.apache.spark.sql.execution.blaze.plan.NativeSortBase
import org.apache.spark.sql.execution.blaze.plan.NativeSortExec
import org.apache.spark.sql.execution.blaze.plan.NativeTakeOrderedBase
import org.apache.spark.sql.execution.blaze.plan.NativeTakeOrderedExec
import org.apache.spark.sql.execution.blaze.plan.NativeUnionBase
import org.apache.spark.sql.execution.blaze.plan.NativeUnionExec
import org.apache.spark.sql.execution.blaze.plan.NativeWindowBase
import org.apache.spark.sql.execution.blaze.plan.NativeWindowExec
import org.apache.spark.sql.execution.blaze.plan._
import org.apache.spark.sql.execution.blaze.shuffle.RssPartitionWriterBase
import org.apache.spark.sql.execution.blaze.shuffle.celeborn.BlazeCelebornShuffleManager
import org.apache.spark.sql.execution.blaze.shuffle.BlazeBlockStoreShuffleReaderBase
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, ReusedExchangeExec}
import org.apache.spark.sql.execution.joins.blaze.plan.NativeBroadcastJoinExec
import org.apache.spark.sql.execution.joins.blaze.plan.NativeShuffledHashJoinExecProvider
import org.apache.spark.sql.execution.joins.blaze.plan.NativeSortMergeJoinExecProvider
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLShuffleReadMetricsReporter}
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.storage.FileSegment
import org.blaze.{protobuf => pb}
import com.thoughtworks.enableIf
import org.apache.spark.sql.execution.blaze.shuffle.uniffle.BlazeUniffleShuffleManager

class ShimsImpl extends Shims with Logging {

  @enableIf(Seq("spark-3.0").contains(System.getProperty("blaze.shim")))
  override def shimVersion: String = "spark-3.0"
  @enableIf(Seq("spark-3.1").contains(System.getProperty("blaze.shim")))
  override def shimVersion: String = "spark-3.1"
  @enableIf(Seq("spark-3.2").contains(System.getProperty("blaze.shim")))
  override def shimVersion: String = "spark-3.2"
  @enableIf(Seq("spark-3.3").contains(System.getProperty("blaze.shim")))
  override def shimVersion: String = "spark-3.3"
  @enableIf(Seq("spark-3.4").contains(System.getProperty("blaze.shim")))
  override def shimVersion: String = "spark-3.4"
  @enableIf(Seq("spark-3.5").contains(System.getProperty("blaze.shim")))
  override def shimVersion: String = "spark-3.5"

  @enableIf(
    Seq("spark-3.2", "spark-3.3", "spark-3.4", "spark-3.5").contains(
      System.getProperty("blaze.shim")))
  override def initExtension(): Unit = {
    ValidateSparkPlanInjector.inject()

    if (BlazeConf.FORCE_SHUFFLED_HASH_JOIN.booleanConf()) {
      ForceApplyShuffledHashJoinInjector.inject()
    }

    // disable MultiCommutativeOp suggested in spark3.4+
    if (shimVersion >= "spark-3.4") {
      val confName = "spark.sql.analyzer.canonicalization.multiCommutativeOpMemoryOptThreshold"
      SparkEnv.get.conf.set(confName, Int.MaxValue.toString)
    }
  }

  @enableIf(Seq("spark-3.0", "spark-3.1").contains(System.getProperty("blaze.shim")))
  override def initExtension(): Unit = {
    if (BlazeConf.FORCE_SHUFFLED_HASH_JOIN.booleanConf()) {
      logWarning(s"${BlazeConf.FORCE_SHUFFLED_HASH_JOIN.key} is not supported in $shimVersion")
    }
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
      broadcastSide: BroadcastSide): NativeBroadcastJoinBase =
    NativeBroadcastJoinExec(
      left,
      right,
      outputPartitioning,
      leftKeys,
      rightKeys,
      joinType,
      broadcastSide)

  override def createNativeSortMergeJoinExec(
      left: SparkPlan,
      right: SparkPlan,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType): NativeSortMergeJoinBase =
    NativeSortMergeJoinExecProvider.provide(left, right, leftKeys, rightKeys, joinType)

  override def createNativeShuffledHashJoinExec(
      left: SparkPlan,
      right: SparkPlan,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      buildSide: BuildSide): SparkPlan =
    NativeShuffledHashJoinExecProvider.provide(
      left,
      right,
      leftKeys,
      rightKeys,
      joinType,
      buildSide)

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

  override def createNativeOrcScanExec(basedFileScan: FileSourceScanExec): NativeOrcScanBase =
    NativeOrcScanExec(basedFileScan)

  override def createNativeProjectExec(
      projectList: Seq[NamedExpression],
      child: SparkPlan,
      addTypeCast: Boolean = false): NativeProjectBase =
    NativeProjectExecProvider.provide(projectList, child, addTypeCast)

  override def createNativeRenameColumnsExec(
      child: SparkPlan,
      newColumnNames: Seq[String]): NativeRenameColumnsBase =
    NativeRenameColumnsExecProvider.provide(child, newColumnNames)

  override def createNativeShuffleExchangeExec(
      outputPartitioning: Partitioning,
      child: SparkPlan,
      shuffleOrigin: Option[Any] = None): NativeShuffleExchangeBase =
    NativeShuffleExchangeExec(outputPartitioning, child, shuffleOrigin)

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
      case plan if isAQEShuffleRead(plan) => isNative(plan.children.head)
      case plan: QueryStageExec => isNative(plan.plan)
      case plan: ReusedExchangeExec => isNative(plan.child)
      case _ => false
    }

  override def getUnderlyingNativePlan(plan: SparkPlan): NativeSupports = {
    plan match {
      case plan: NativeSupports => plan
      case plan if isAQEShuffleRead(plan) => getUnderlyingNativePlan(plan.children.head)
      case plan: QueryStageExec => getUnderlyingNativePlan(plan.plan)
      case plan: ReusedExchangeExec => getUnderlyingNativePlan(plan.child)
      case _ => throw new RuntimeException("unreachable: plan is not native")
    }
  }

  override def executeNative(plan: SparkPlan): NativeRDD = {
    plan match {
      case plan: NativeSupports => plan.executeNative()
      case plan if isAQEShuffleRead(plan) => executeNativeAQEShuffleReader(plan)
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

  override def getRDDShuffleReadFull(rdd: RDD[_]): Boolean = true

  override def setRDDShuffleReadFull(rdd: RDD[_], shuffleReadFull: Boolean): Unit = {}

  override def createFileSegment(
      file: File,
      offset: Long,
      length: Long,
      numRecords: Long): FileSegment = new FileSegment(file, offset, length)

  @enableIf(
    Seq("spark-3.2", "spark-3.3", "spark-3.4", "spark-3.5").contains(
      System.getProperty("blaze.shim")))
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

  @enableIf(Seq("spark-3.0", "spark-3.1").contains(System.getProperty("blaze.shim")))
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
      nativeOutputPartitioning: pb.PhysicalRepartition.Builder): pb.PhysicalPlanNode = {

    if (SparkEnv.get.shuffleManager
        .isInstanceOf[BlazeCelebornShuffleManager] || SparkEnv.get.shuffleManager
        .isInstanceOf[BlazeUniffleShuffleManager]) {
      return pb.PhysicalPlanNode
        .newBuilder()
        .setRssShuffleWriter(
          pb.RssShuffleWriterExecNode
            .newBuilder()
            .setInput(input)
            .setOutputPartitioning(nativeOutputPartitioning)
            .buildPartial()
        ) // shuffleId is not set at the moment, will be set in ShuffleWriteProcessor
        .build()
    }

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

  override def convertMoreExprWithFallback(
      e: Expression,
      isPruningExpr: Boolean,
      fallback: Expression => pb.PhysicalExprNode): Option[pb.PhysicalExprNode] = {
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
                .addArgs(NativeConverters.convertExprWithFallback(str, isPruningExpr, fallback))
                .addArgs(NativeConverters
                  .convertExprWithFallback(Literal(nativePat), isPruningExpr, fallback))
                .setReturnType(NativeConverters.convertDataType(StringType)))
            .build())

      case e: TaggingExpression =>
        Some(NativeConverters.convertExprWithFallback(e.child, isPruningExpr, fallback))
      case e =>
        convertPromotePrecision(e, isPruningExpr, fallback) match {
          case Some(v) => return Some(v)
          case None =>
        }
        convertBloomFilterMightContain(e, isPruningExpr, fallback) match {
          case Some(v) => return Some(v)
          case None =>
        }
        None
    }
  }

  override def getLikeEscapeChar(expr: Expression): Char = {
    expr.asInstanceOf[Like].escapeChar
  }

  override def convertMoreAggregateExpr(e: AggregateExpression): Option[pb.PhysicalExprNode] = {
    assert(getAggregateExpressionFilter(e).isEmpty)

    e.aggregateFunction match {
      case First(child, ignoresNull) =>
        val aggExpr = pb.PhysicalAggExprNode
          .newBuilder()
          .setAggFunction(if (ignoresNull) {
            pb.AggFunction.FIRST_IGNORES_NULL
          } else {
            pb.AggFunction.FIRST
          })
          .addChildren(NativeConverters.convertExpr(child))
        Some(pb.PhysicalExprNode.newBuilder().setAggExpr(aggExpr).build())

      case agg =>
        convertBloomFilterAgg(agg) match {
          case Some(aggExpr) =>
            return Some(pb.PhysicalExprNode.newBuilder().setAggExpr(aggExpr).build())
          case None =>
        }
        None
    }
  }

  override def getAggregateExpressionFilter(expr: Expression): Option[Expression] = {
    expr.asInstanceOf[AggregateExpression].filter
  }

  @enableIf(
    Seq("spark-3.2", "spark-3.3", "spark-3.4", "spark-3.5").contains(
      System.getProperty("blaze.shim")))
  private def isAQEShuffleRead(exec: SparkPlan): Boolean = {
    import org.apache.spark.sql.execution.adaptive.AQEShuffleReadExec
    exec.isInstanceOf[AQEShuffleReadExec]
  }

  @enableIf(Seq("spark-3.0", "spark-3.1").contains(System.getProperty("blaze.shim")))
  private def isAQEShuffleRead(exec: SparkPlan): Boolean = {
    import org.apache.spark.sql.execution.adaptive.CustomShuffleReaderExec
    exec.isInstanceOf[CustomShuffleReaderExec]
  }

  @enableIf(
    Seq("spark-3.2", "spark-3.3", "spark-3.4", "spark-3.5").contains(
      System.getProperty("blaze.shim")))
  private def executeNativeAQEShuffleReader(exec: SparkPlan): NativeRDD = {
    import org.apache.spark.sql.execution.adaptive.AQEShuffleReadExec
    import org.apache.spark.sql.execution.CoalescedMapperPartitionSpec

    exec match {
      case AQEShuffleReadExec(child, _) if isNative(child) =>
        val shuffledRDD = exec.execute().asInstanceOf[ShuffledRowRDD]
        val shuffleHandle = shuffledRDD.dependency.shuffleHandle

        val inputRDD = executeNative(child)
        val nativeShuffle = getUnderlyingNativePlan(child).asInstanceOf[NativeShuffleExchangeExec]
        val nativeSchema: pb.Schema = nativeShuffle.nativeSchema

        val requiredMetrics = nativeShuffle.readMetrics ++
          nativeShuffle.metrics.filterKeys(_ == "shuffle_read_total_time")
        val metrics = MetricNode(
          requiredMetrics,
          inputRDD.metrics :: Nil,
          Some({
            case ("output_rows", v) =>
              val tempMetrics = TaskContext.get.taskMetrics().createTempShuffleReadMetrics()
              new SQLShuffleReadMetricsReporter(tempMetrics, requiredMetrics).incRecordsRead(v)
              TaskContext.get().taskMetrics().mergeShuffleReadMetrics()
            case ("elapsed_compute", v) => requiredMetrics("shuffle_read_total_time") += v
            case _ =>
          }))

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
                reader.asInstanceOf[BlazeBlockStoreShuffleReaderBase[_, _]].readIpc()
              })

            pb.PhysicalPlanNode
              .newBuilder()
              .setIpcReader(
                pb.IpcReaderExecNode
                  .newBuilder()
                  .setSchema(nativeSchema)
                  .setNumPartitions(shuffledRDD.getNumPartitions)
                  .setIpcProviderResourceId(jniResourceId)
                  .build())
              .build()
          })
    }
  }

  @enableIf(Seq("spark-3.1").contains(System.getProperty("blaze.shim")))
  private def executeNativeAQEShuffleReader(exec: SparkPlan): NativeRDD = {
    import org.apache.spark.sql.execution.adaptive.CustomShuffleReaderExec

    exec match {
      case CustomShuffleReaderExec(child, _) if isNative(child) =>
        val shuffledRDD = exec.execute().asInstanceOf[ShuffledRowRDD]
        val shuffleHandle = shuffledRDD.dependency.shuffleHandle

        val inputRDD = executeNative(child)
        val nativeShuffle = getUnderlyingNativePlan(child).asInstanceOf[NativeShuffleExchangeExec]
        val nativeSchema: pb.Schema = nativeShuffle.nativeSchema

        val requiredMetrics = nativeShuffle.readMetrics ++
          nativeShuffle.metrics.filterKeys(_ == "shuffle_read_total_time")
        val metrics = MetricNode(
          requiredMetrics,
          inputRDD.metrics :: Nil,
          Some({
            case ("output_rows", v) =>
              val tempMetrics = TaskContext.get.taskMetrics().createTempShuffleReadMetrics()
              new SQLShuffleReadMetricsReporter(tempMetrics, requiredMetrics).incRecordsRead(v)
              TaskContext.get().taskMetrics().mergeShuffleReadMetrics()
            case ("elapsed_compute", v) => requiredMetrics("shuffle_read_total_time") += v
            case _ =>
          }))

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
              case CoalescedPartitionSpec(startReducerIndex, endReducerIndex) =>
                SparkEnv.get.shuffleManager.getReader(
                  shuffleHandle,
                  startReducerIndex,
                  endReducerIndex,
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
                reader.asInstanceOf[BlazeBlockStoreShuffleReaderBase[_, _]].readIpc()
              })

            pb.PhysicalPlanNode
              .newBuilder()
              .setIpcReader(
                pb.IpcReaderExecNode
                  .newBuilder()
                  .setSchema(nativeSchema)
                  .setNumPartitions(shuffledRDD.getNumPartitions)
                  .setIpcProviderResourceId(jniResourceId)
                  .build())
              .build()
          })
    }
  }

  @enableIf(Seq("spark-3.0").contains(System.getProperty("blaze.shim")))
  private def executeNativeAQEShuffleReader(exec: SparkPlan): NativeRDD = {
    import org.apache.spark.sql.execution.adaptive.CustomShuffleReaderExec

    exec match {
      case CustomShuffleReaderExec(child, _, _) if isNative(child) =>
        val shuffledRDD = exec.execute().asInstanceOf[ShuffledRowRDD]
        val shuffleHandle = shuffledRDD.dependency.shuffleHandle

        val inputRDD = executeNative(child)
        val nativeShuffle = getUnderlyingNativePlan(child).asInstanceOf[NativeShuffleExchangeExec]
        val nativeSchema: pb.Schema = nativeShuffle.nativeSchema

        val requiredMetrics = nativeShuffle.readMetrics ++
          nativeShuffle.metrics.filterKeys(_ == "shuffle_read_total_time")
        val metrics = MetricNode(
          requiredMetrics,
          inputRDD.metrics :: Nil,
          Some({
            case ("output_rows", v) =>
              val tempMetrics = TaskContext.get.taskMetrics().createTempShuffleReadMetrics()
              new SQLShuffleReadMetricsReporter(tempMetrics, requiredMetrics).incRecordsRead(v)
              TaskContext.get().taskMetrics().mergeShuffleReadMetrics()
            case ("elapsed_compute", v) => requiredMetrics("shuffle_read_total_time") += v
            case _ =>
          }))

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
              case CoalescedPartitionSpec(startReducerIndex, endReducerIndex) =>
                SparkEnv.get.shuffleManager.getReader(
                  shuffleHandle,
                  startReducerIndex,
                  endReducerIndex,
                  taskContext,
                  sqlMetricsReporter)

              case PartialReducerPartitionSpec(reducerIndex, startMapIndex, endMapIndex) =>
                SparkEnv.get.shuffleManager.getReaderForRange(
                  shuffleHandle,
                  startMapIndex,
                  endMapIndex,
                  reducerIndex,
                  reducerIndex + 1,
                  taskContext,
                  sqlMetricsReporter)

              case PartialMapperPartitionSpec(mapIndex, startReducerIndex, endReducerIndex) =>
                SparkEnv.get.shuffleManager.getReaderForRange(
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
                reader.asInstanceOf[BlazeBlockStoreShuffleReaderBase[_, _]].readIpc()
              })

            pb.PhysicalPlanNode
              .newBuilder()
              .setIpcReader(
                pb.IpcReaderExecNode
                  .newBuilder()
                  .setSchema(nativeSchema)
                  .setNumPartitions(shuffledRDD.getNumPartitions)
                  .setIpcProviderResourceId(jniResourceId)
                  .build())
              .build()
          })
    }
  }

  override def convertMoreSparkPlan(exec: SparkPlan): Option[SparkPlan] = {
    exec match {
      case exec if isAQEShuffleRead(exec) && isNative(exec) =>
        Some(ForceNativeExecutionWrapper(BlazeConverters.addRenameColumnsExec(exec)))
      case _: ReusedExchangeExec if isNative(exec) =>
        Some(ForceNativeExecutionWrapper(BlazeConverters.addRenameColumnsExec(exec)))
      case _ => None
    }
  }

  @enableIf(
    Seq("spark-3.2", "spark-3.3", "spark-3.4", "spark-3.5").contains(
      System.getProperty("blaze.shim")))
  override def getSqlContext(sparkPlan: SparkPlan): SQLContext =
    sparkPlan.session.sqlContext

  @enableIf(Seq("spark-3.0", "spark-3.1").contains(System.getProperty("blaze.shim")))
  override def getSqlContext(sparkPlan: SparkPlan): SQLContext = sparkPlan.sqlContext

  override def createNativeExprWrapper(
      nativeExpr: pb.PhysicalExprNode,
      dataType: DataType,
      nullable: Boolean): Expression = {
    NativeExprWrapper(nativeExpr, dataType, nullable)
  }

  @enableIf(
    Seq("spark-3.0", "spark-3.1", "spark-3.2", "spark-3.3").contains(
      System.getProperty("blaze.shim")))
  override def getPartitionedFile(
      partitionValues: InternalRow,
      filePath: String,
      offset: Long,
      size: Long): PartitionedFile =
    PartitionedFile(partitionValues, filePath, offset, size)

  @enableIf(Seq("spark-3.4", "spark-3.5").contains(System.getProperty("blaze.shim")))
  override def getPartitionedFile(
      partitionValues: InternalRow,
      filePath: String,
      offset: Long,
      size: Long): PartitionedFile = {
    import org.apache.hadoop.fs.Path
    import org.apache.spark.paths.SparkPath
    PartitionedFile(partitionValues, SparkPath.fromPath(new Path(filePath)), offset, size)
  }

  @enableIf(
    Seq("spark-3.1", "spark-3.2", "spark-3.3", "spark-3.4", "spark-3.5").contains(
      System.getProperty("blaze.shim")))
  override def getMinPartitionNum(sparkSession: SparkSession): Int =
    sparkSession.sessionState.conf.filesMinPartitionNum
      .getOrElse(sparkSession.sparkContext.defaultParallelism)

  @enableIf(Seq("spark-3.0").contains(System.getProperty("blaze.shim")))
  override def getMinPartitionNum(sparkSession: SparkSession): Int =
    sparkSession.sparkContext.defaultParallelism

  @enableIf(
    Seq("spark-3.0", "spark-3.1", "spark-3.2", "spark-3.3").contains(
      System.getProperty("blaze.shim")))
  private def convertPromotePrecision(
      e: Expression,
      isPruningExpr: Boolean,
      fallback: Expression => pb.PhysicalExprNode): Option[pb.PhysicalExprNode] = {
    import org.apache.spark.sql.catalyst.expressions.PromotePrecision
    e match {
      case PromotePrecision(_1) =>
        Some(NativeConverters.convertExprWithFallback(_1, isPruningExpr, fallback))
      case _ => None
    }
  }

  @enableIf(Seq("spark-3.4", "spark-3.5").contains(System.getProperty("blaze.shim")))
  private def convertPromotePrecision(
      e: Expression,
      isPruningExpr: Boolean,
      fallback: Expression => pb.PhysicalExprNode): Option[pb.PhysicalExprNode] = None

  @enableIf(Seq("spark-3.3", "spark-3.4", "spark-3.5").contains(System.getProperty("blaze.shim")))
  private def convertBloomFilterAgg(agg: AggregateFunction): Option[pb.PhysicalAggExprNode] = {
    import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
    agg match {
      case BloomFilterAggregate(child, estimatedNumItemsExpression, numBitsExpression, _, _) =>
        // ensure numBits is a power of 2
        val estimatedNumItems =
          estimatedNumItemsExpression.eval().asInstanceOf[Number].longValue()
        val numBits = numBitsExpression.eval().asInstanceOf[Number].longValue()
        val numBitsNextPowerOf2 = numBits match {
          case 1 => 1L
          case n => Integer.highestOneBit(n.toInt - 1) << 1
        }
        Some(
          pb.PhysicalAggExprNode
            .newBuilder()
            .setAggFunction(pb.AggFunction.BLOOM_FILTER)
            .addChildren(NativeConverters.convertExpr(child))
            .addChildren(NativeConverters.convertExpr(Literal(estimatedNumItems)))
            .addChildren(NativeConverters.convertExpr(Literal(numBitsNextPowerOf2)))
            .build())
      case _ => None
    }
  }

  @enableIf(Seq("spark-3.0", "spark-3.1", "spark-3.2").contains(System.getProperty("blaze.shim")))
  private def convertBloomFilterAgg(agg: AggregateFunction): Option[pb.PhysicalAggExprNode] = None

  @enableIf(Seq("spark-3.3", "spark-3.4", "spark-3.5").contains(System.getProperty("blaze.shim")))
  private def convertBloomFilterMightContain(
      e: Expression,
      isPruningExpr: Boolean,
      fallback: Expression => pb.PhysicalExprNode): Option[pb.PhysicalExprNode] = {
    import org.apache.spark.sql.catalyst.expressions.BloomFilterMightContain
    e match {
      case e: BloomFilterMightContain =>
        val uuid = UUID.randomUUID().toString
        Some(NativeConverters.buildExprNode {
          _.setBloomFilterMightContainExpr(
            pb.BloomFilterMightContainExprNode
              .newBuilder()
              .setUuid(uuid)
              .setBloomFilterExpr(NativeConverters
                .convertExprWithFallback(e.bloomFilterExpression, isPruningExpr, fallback))
              .setValueExpr(NativeConverters
                .convertExprWithFallback(e.valueExpression, isPruningExpr, fallback)))
        })
      case _ => None
    }
  }

  @enableIf(Seq("spark-3.0", "spark-3.1", "spark-3.2").contains(System.getProperty("blaze.shim")))
  private def convertBloomFilterMightContain(
      e: Expression,
      isPruningExpr: Boolean,
      fallback: Expression => pb.PhysicalExprNode): Option[pb.PhysicalExprNode] = None

}

case class ForceNativeExecutionWrapper(override val child: SparkPlan)
    extends ForceNativeExecutionWrapperBase(child) {

  @enableIf(
    Seq("spark-3.2", "spark-3.3", "spark-3.4", "spark-3.5").contains(
      System.getProperty("blaze.shim")))
  override def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)

  @enableIf(Seq("spark-3.0", "spark-3.1").contains(System.getProperty("blaze.shim")))
  override def withNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
    copy(child = newChildren.head)
}

case class NativeExprWrapper(
    nativeExpr: pb.PhysicalExprNode,
    override val dataType: DataType,
    override val nullable: Boolean)
    extends NativeExprWrapperBase(nativeExpr, dataType, nullable) {

  @enableIf(
    Seq("spark-3.2", "spark-3.3", "spark-3.4", "spark-3.5").contains(
      System.getProperty("blaze.shim")))
  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = copy()
}
