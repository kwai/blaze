/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.auron

import java.util.ServiceLoader

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.commons.lang3.reflect.MethodUtils
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat
import org.apache.spark.Partition
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.auron.AuronConvertStrategy.childOrderingRequiredTag
import org.apache.spark.sql.auron.AuronConvertStrategy.convertibleTag
import org.apache.spark.sql.auron.AuronConvertStrategy.convertStrategyTag
import org.apache.spark.sql.auron.AuronConvertStrategy.convertToNonNativeTag
import org.apache.spark.sql.auron.AuronConvertStrategy.isNeverConvert
import org.apache.spark.sql.auron.AuronConvertStrategy.joinSmallerSideTag
import org.apache.spark.sql.auron.NativeConverters.{roundRobinTypeSupported, scalarTypeSupported, StubExpr}
import org.apache.spark.sql.auron.util.AuronLogUtils.logDebugPlanConversion
import org.apache.spark.sql.catalyst.expressions.AggregateWindowFunction
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.expressions.WindowExpression
import org.apache.spark.sql.catalyst.expressions.WindowSpecDefinition
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.expressions.aggregate.Final
import org.apache.spark.sql.catalyst.expressions.aggregate.Partial
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.plans.physical.RangePartitioning
import org.apache.spark.sql.catalyst.plans.physical.RoundRobinPartitioning
import org.apache.spark.sql.execution.ExpandExec
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.FilterExec
import org.apache.spark.sql.execution.GenerateExec
import org.apache.spark.sql.execution.GlobalLimitExec
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.LocalLimitExec
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.execution.SortExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.TakeOrderedAndProjectExec
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.UnionExec
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.aggregate.ObjectHashAggregateExec
import org.apache.spark.sql.execution.aggregate.SortAggregateExec
import org.apache.spark.sql.execution.auron.plan.BroadcastLeft
import org.apache.spark.sql.execution.auron.plan.BroadcastRight
import org.apache.spark.sql.execution.auron.plan.ConvertToNativeBase
import org.apache.spark.sql.execution.auron.plan.NativeAggBase
import org.apache.spark.sql.execution.auron.plan.NativeBroadcastExchangeBase
import org.apache.spark.sql.execution.auron.plan.NativeOrcScanBase
import org.apache.spark.sql.execution.auron.plan.NativeParquetScanBase
import org.apache.spark.sql.execution.auron.plan.NativeSortBase
import org.apache.spark.sql.execution.auron.plan.NativeUnionBase
import org.apache.spark.sql.execution.auron.plan.Util
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable
import org.apache.spark.sql.hive.execution.auron.plan.NativeHiveTableScanBase
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.LongType

import org.apache.auron.protobuf.EmptyPartitionsExecNode
import org.apache.auron.protobuf.PhysicalPlanNode
import org.apache.auron.sparkver

object AuronConverters extends Logging {
  private def enableScan: Boolean = AuronConf.ENABLE_SCAN.booleanConf()
  def enableProject: Boolean = AuronConf.ENABLE_PROJECT.booleanConf()
  def enableFilter: Boolean = AuronConf.ENABLE_FILTER.booleanConf()
  def enableSort: Boolean = AuronConf.ENABLE_SORT.booleanConf()
  def enableUnion: Boolean = AuronConf.ENABLE_UNION.booleanConf()
  def enableSmj: Boolean = AuronConf.ENABLE_SMJ.booleanConf()
  def enableShj: Boolean = AuronConf.ENABLE_SHJ.booleanConf()
  def enableBhj: Boolean = AuronConf.ENABLE_BHJ.booleanConf()
  def enableBnlj: Boolean = AuronConf.ENABLE_BNLJ.booleanConf()
  def enableLocalLimit: Boolean = AuronConf.ENABLE_LOCAL_LIMIT.booleanConf()
  def enableGlobalLimit: Boolean = AuronConf.ENABLE_GLOBAL_LIMIT.booleanConf()
  def enableTakeOrderedAndProject: Boolean =
    AuronConf.ENABLE_TAKE_ORDERED_AND_PROJECT.booleanConf()
  def enableAggr: Boolean = AuronConf.ENABLE_AGGR.booleanConf()
  def enableExpand: Boolean = AuronConf.ENABLE_EXPAND.booleanConf()
  def enableWindow: Boolean = AuronConf.ENABLE_WINDOW.booleanConf()
  def enableWindowGroupLimit: Boolean = AuronConf.ENABLE_WINDOW_GROUP_LIMIT.booleanConf()
  def enableGenerate: Boolean = AuronConf.ENABLE_GENERATE.booleanConf()
  def enableLocalTableScan: Boolean = AuronConf.ENABLE_LOCAL_TABLE_SCAN.booleanConf()
  def enableDataWriting: Boolean = AuronConf.ENABLE_DATA_WRITING.booleanConf()
  def enableScanParquet: Boolean = AuronConf.ENABLE_SCAN_PARQUET.booleanConf()
  def enableScanOrc: Boolean = AuronConf.ENABLE_SCAN_ORC.booleanConf()

  private val extConvertProviders = ServiceLoader.load(classOf[AuronConvertProvider]).asScala
  def extConvertSupported(exec: SparkPlan): Boolean = {
    extConvertProviders.exists(_.isSupported(exec))
  }

  // format: off
  // scalafix:off
  // necessary imports for cross spark versions build
  import org.apache.spark.sql.catalyst.plans._
  import org.apache.spark.sql.catalyst.optimizer._
  // scalafix:on
  // format: on

  def convertSparkPlanRecursively(exec: SparkPlan): SparkPlan = {
    // convert
    var danglingConverted: Seq[SparkPlan] = Nil
    exec.foreachUp { exec =>
      val (newDanglingConverted, newChildren) =
        danglingConverted.splitAt(danglingConverted.length - exec.children.length)

      var newExec = exec.withNewChildren(newChildren)
      exec.getTagValue(convertibleTag).foreach(newExec.setTagValue(convertibleTag, _))
      exec.getTagValue(convertStrategyTag).foreach(newExec.setTagValue(convertStrategyTag, _))
      exec
        .getTagValue(childOrderingRequiredTag)
        .foreach(newExec.setTagValue(childOrderingRequiredTag, _))
      exec.getTagValue(joinSmallerSideTag).foreach(newExec.setTagValue(joinSmallerSideTag, _))

      if (!isNeverConvert(newExec)) {
        newExec = convertSparkPlan(newExec)
      }
      danglingConverted = newDanglingConverted :+ newExec
    }
    danglingConverted.head
  }

  def convertSparkPlan(exec: SparkPlan): SparkPlan = {
    exec match {
      case e: ShuffleExchangeExec => tryConvert(e, convertShuffleExchangeExec)
      case e: BroadcastExchangeExec => tryConvert(e, convertBroadcastExchangeExec)
      case e: FileSourceScanExec if enableScan => // scan
        tryConvert(e, convertFileSourceScanExec)
      case e: ProjectExec if enableProject => // project
        tryConvert(e, convertProjectExec)
      case e: FilterExec if enableFilter => // filter
        tryConvert(e, convertFilterExec)
      case e: SortExec if enableSort => // sort
        tryConvert(e, convertSortExec)
      case e: UnionExec if enableUnion => // union
        tryConvert(e, convertUnionExec)
      case e: SortMergeJoinExec if enableSmj => // sort merge join
        tryConvert(e, convertSortMergeJoinExec)
      case e: ShuffledHashJoinExec if enableShj => // shuffled hash join
        tryConvert(e, convertShuffledHashJoinExec)
      case e: BroadcastHashJoinExec if enableBhj => // broadcast hash join
        tryConvert(e, convertBroadcastHashJoinExec)
      case e: BroadcastNestedLoopJoinExec if enableBnlj => // broadcast nested loop join
        tryConvert(e, convertBroadcastNestedLoopJoinExec)
      case e: LocalLimitExec if enableLocalLimit => // local limit
        tryConvert(e, convertLocalLimitExec)
      case e: GlobalLimitExec if enableGlobalLimit => // global limit
        tryConvert(e, convertGlobalLimitExec)
      case e: TakeOrderedAndProjectExec if enableTakeOrderedAndProject =>
        tryConvert(e, convertTakeOrderedAndProjectExec)

      case e: HashAggregateExec if enableAggr => // hash aggregate
        val convertedAgg = tryConvert(e, convertHashAggregateExec)
        if (!e.getTagValue(convertibleTag).contains(true)) {
          if (e.requiredChildDistributionExpressions.isDefined) {
            assert(
              NativeAggBase.findPreviousNativeAggrExec(e).isEmpty,
              "native agg followed by non-native agg is forbidden")
          }
        }
        convertedAgg

      case e: ObjectHashAggregateExec if enableAggr => // object hash aggregate
        val convertedAgg = tryConvert(e, convertObjectHashAggregateExec)
        if (!e.getTagValue(convertibleTag).contains(true)) {
          if (e.requiredChildDistributionExpressions.isDefined) {
            assert(
              NativeAggBase.findPreviousNativeAggrExec(e).isEmpty,
              "native agg followed by non-native agg is forbidden")
          }
        }
        convertedAgg

      case e: SortAggregateExec if enableAggr => // sort aggregate
        val convertedAgg = tryConvert(e, convertSortAggregateExec)
        if (!e.getTagValue(convertibleTag).contains(true)) {
          if (e.requiredChildDistributionExpressions.isDefined) {
            assert(
              NativeAggBase.findPreviousNativeAggrExec(e).isEmpty,
              "native agg followed by non-native agg is forbidden")
          }
        }
        convertedAgg

      case e: ExpandExec if enableExpand => // expand
        tryConvert(e, convertExpandExec)
      case e: WindowExec if enableWindow => // window
        tryConvert(e, convertWindowExec)
      case e: UnaryExecNode
          if e.getClass.getSimpleName == "WindowGroupLimitExec" && enableWindowGroupLimit => // window group limit
        tryConvert(e, convertWindowGroupLimitExec)
      case e: GenerateExec if enableGenerate => // generate
        tryConvert(e, convertGenerateExec)
      case e: LocalTableScanExec if enableLocalTableScan => // local table scan
        tryConvert(e, convertLocalTableScanExec)
      case e: DataWritingCommandExec if enableDataWriting => // data writing
        tryConvert(e, convertDataWritingCommandExec)

      case exec: ForceNativeExecutionWrapperBase => exec
      case exec =>
        extConvertProviders.find(h => h.isEnabled && h.isSupported(exec)) match {
          case Some(provider) => tryConvert(exec, provider.convert)
          case None =>
            Shims.get.convertMoreSparkPlan(exec) match {
              case Some(exec) =>
                exec.setTagValue(convertibleTag, true)
                exec.setTagValue(convertStrategyTag, AlwaysConvert)
                exec
              case None =>
                if (Shims.get.isNative(exec)) { // for QueryStageInput and CustomShuffleReader
                  exec.setTagValue(convertibleTag, true)
                  exec.setTagValue(convertStrategyTag, AlwaysConvert)
                } else {
                  exec.setTagValue(convertibleTag, false)
                  exec.setTagValue(convertStrategyTag, NeverConvert)
                }
                exec
            }
        }
    }
  }

  def tryConvert[T <: SparkPlan](exec: T, convert: T => SparkPlan): SparkPlan = {
    try {
      exec.setTagValue(convertibleTag, true)
      convert(exec)

    } catch {
      case e @ (_: NotImplementedError | _: AssertionError | _: Exception) =>
        logWarning(s"Falling back exec: ${exec.getClass.getSimpleName}: ${e.getMessage}")
        exec.setTagValue(convertibleTag, false)
        exec.setTagValue(convertStrategyTag, NeverConvert)
        exec
    }
  }

  def convertShuffleExchangeExec(exec: ShuffleExchangeExec): SparkPlan = {
    val (outputPartitioning, child) = (exec.outputPartitioning, exec.child)
    logDebugPlanConversion(exec)

    assert(
      outputPartitioning.numPartitions == 1 || outputPartitioning
        .isInstanceOf[HashPartitioning] || outputPartitioning
        .isInstanceOf[RoundRobinPartitioning] || outputPartitioning
        .isInstanceOf[RangePartitioning],
      s"partitioning not supported: $outputPartitioning")

    outputPartitioning match {
      case partitioning: RangePartitioning =>
        val unsupportedOrderType = partitioning.ordering
          .find(e => !scalarTypeSupported(e.dataType))
        assert(
          unsupportedOrderType.isEmpty,
          s"Unsupported order type in range partitioning: ${unsupportedOrderType.get}")
      case _: RoundRobinPartitioning =>
        val unsupportedTypeInRR =
          exec.output.find(attr => !roundRobinTypeSupported(attr.dataType))
        assert(
          unsupportedTypeInRR.isEmpty,
          s"Unsupported data type in $outputPartitioning: attribute=${unsupportedTypeInRR.get.name}" +
            s", dataType=${unsupportedTypeInRR.get.dataType}")
      case _ =>
    }

    val convertedChild = outputPartitioning match {
      case p
          if p.isInstanceOf[HashPartitioning] || p
            .isInstanceOf[RoundRobinPartitioning] || p.isInstanceOf[RangePartitioning]
            || p.numPartitions == 1 =>
        convertToNative(child)
      case _ => child
    }
    Shims.get.createNativeShuffleExchangeExec(
      outputPartitioning,
      addRenameColumnsExec(convertedChild),
      getShuffleOrigin(exec))
  }

  @sparkver(" 3.2 / 3.3 / 3.4 / 3.5")
  def getIsSkewJoinFromSHJ(exec: ShuffledHashJoinExec): Boolean = exec.isSkewJoin

  @sparkver("3.0 / 3.1")
  def getIsSkewJoinFromSHJ(exec: ShuffledHashJoinExec): Boolean = false

  @sparkver("3.1 / 3.2 / 3.3 / 3.4 / 3.5")
  def getShuffleOrigin(exec: ShuffleExchangeExec): Option[Any] = Some(exec.shuffleOrigin)

  @sparkver("3.0")
  def getShuffleOrigin(exec: ShuffleExchangeExec): Option[Any] = None

  def convertFileSourceScanExec(exec: FileSourceScanExec): SparkPlan = {
    val (
      relation,
      output,
      requiredSchema,
      partitionFilters,
      optionalBucketSet,
      dataFilters,
      tableIdentifier) = (
      exec.relation,
      exec.output,
      exec.requiredSchema,
      exec.partitionFilters,
      exec.optionalBucketSet,
      exec.dataFilters,
      exec.tableIdentifier)
    logDebugPlanConversion(
      exec,
      Seq(
        "relation" -> relation,
        "relation.location" -> relation.location,
        "output" -> output,
        "requiredSchema" -> requiredSchema,
        "partitionFilters" -> partitionFilters,
        "optionalBucketSet" -> optionalBucketSet,
        "dataFilters" -> dataFilters,
        "tableIdentifier" -> tableIdentifier))
    relation.fileFormat match {
      case p if p.getClass.getName.endsWith("ParquetFileFormat") =>
        assert(enableScanParquet)
        addRenameColumnsExec(Shims.get.createNativeParquetScanExec(exec))
      case p if p.getClass.getName.endsWith("OrcFileFormat") =>
        assert(enableScanOrc)
        addRenameColumnsExec(Shims.get.createNativeOrcScanExec(exec))
      case p =>
        throw new NotImplementedError(
          s"Cannot convert FileSourceScanExec tableIdentifier: ${tableIdentifier.getOrElse(
            "unknown")}, class: ${p.getClass.getName}")
    }
  }

  def convertProjectExec(exec: ProjectExec): SparkPlan = {
    val (projectList, child) = (exec.projectList, exec.child)
    if (log.isDebugEnabled) {
      logDebugPlanConversion(exec, Seq("projectExprs" -> projectList.mkString("[", ", ", "]")))
    }
    Shims.get.createNativeProjectExec(projectList, addRenameColumnsExec(convertToNative(child)))
  }

  def convertFilterExec(exec: FilterExec): SparkPlan = {
    logDebugPlanConversion(exec, Seq("condition" -> exec.condition))
    Shims.get.createNativeFilterExec(
      exec.condition,
      addRenameColumnsExec(convertToNative(exec.child)))
  }

  def convertSortExec(exec: SortExec): SparkPlan = {
    val (sortOrder, global, child) = (exec.sortOrder, exec.global, exec.child)
    logDebugPlanConversion(
      exec,
      Seq("global" -> global, "sortOrder" -> sortOrder.mkString("[", ", ", "]")))
    Shims.get.createNativeSortExec(
      sortOrder,
      global,
      addRenameColumnsExec(convertToNative(child)))
  }

  def convertUnionExec(exec: UnionExec): SparkPlan = {
    logDebugPlanConversion(exec)
    Shims.get.createNativeUnionExec(
      exec.children.map(child => addRenameColumnsExec(convertToNative(child))),
      exec.output)
  }

  def convertSortMergeJoinExec(exec: SortMergeJoinExec): SparkPlan = {
    val requireOrdering = exec.getTagValue(childOrderingRequiredTag).contains(true)

    // force shuffled-hash join
    if (!requireOrdering
      && AuronConf.FORCE_SHUFFLED_HASH_JOIN.booleanConf()
      && exec.children.forall(_.isInstanceOf[NativeSortBase])) {
      val (leftKeys, rightKeys, joinType, condition, left, right, isSkewJoin) =
        (
          exec.leftKeys,
          exec.rightKeys,
          exec.joinType,
          exec.condition,
          exec.left,
          exec.right,
          exec.isSkewJoin)
      logDebugPlanConversion(
        exec,
        Seq(
          "leftKeys" -> leftKeys,
          "rightKeys" -> rightKeys,
          "joinType" -> joinType,
          "condition" -> condition))
      assert(condition.isEmpty, "join condition is not supported")

      val buildSide = exec.getTagValue(joinSmallerSideTag) match {
        case Some(org.apache.spark.sql.execution.auron.plan.BuildLeft) =>
          org.apache.spark.sql.execution.auron.plan.BuildLeft
        case Some(org.apache.spark.sql.execution.auron.plan.BuildRight) =>
          org.apache.spark.sql.execution.auron.plan.BuildRight
        case None =>
          logWarning("JoinSmallerSideTag is missing, defaults to BuildRight")
          org.apache.spark.sql.execution.auron.plan.BuildRight
      }
      return Shims.get.createNativeShuffledHashJoinExec(
        addRenameColumnsExec(convertToNative(left.children(0))),
        addRenameColumnsExec(convertToNative(right.children(0))),
        leftKeys,
        rightKeys,
        joinType,
        buildSide,
        isSkewJoin)
    }

    val (leftKeys, rightKeys, joinType, condition, left, right, isSkewJoin) =
      (
        exec.leftKeys,
        exec.rightKeys,
        exec.joinType,
        exec.condition,
        exec.left,
        exec.right,
        exec.isSkewJoin)
    logDebugPlanConversion(
      exec,
      Seq(
        "leftKeys" -> leftKeys,
        "rightKeys" -> rightKeys,
        "joinType" -> joinType,
        "condition" -> condition))
    assert(condition.isEmpty, "join condition is not supported")

    Shims.get.createNativeSortMergeJoinExec(
      addRenameColumnsExec(convertToNative(left)),
      addRenameColumnsExec(convertToNative(right)),
      leftKeys,
      rightKeys,
      joinType,
      isSkewJoin)
  }

  def convertShuffledHashJoinExec(exec: ShuffledHashJoinExec): SparkPlan = {
    val (leftKeys, rightKeys, joinType, condition, left, right, buildSide) = (
      exec.leftKeys,
      exec.rightKeys,
      exec.joinType,
      exec.condition,
      exec.left,
      exec.right,
      exec.buildSide)
    logDebugPlanConversion(
      exec,
      Seq(
        "leftKeys" -> leftKeys,
        "rightKeys" -> rightKeys,
        "joinType" -> joinType,
        "condition" -> condition,
        "buildSide" -> buildSide))
    try {
      assert(condition.isEmpty, "join condition is not supported")
      Shims.get.createNativeShuffledHashJoinExec(
        addRenameColumnsExec(convertToNative(left)),
        addRenameColumnsExec(convertToNative(right)),
        leftKeys,
        rightKeys,
        joinType,
        buildSide match {
          case BuildLeft => org.apache.spark.sql.execution.auron.plan.BuildLeft
          case BuildRight => org.apache.spark.sql.execution.auron.plan.BuildRight
        },
        getIsSkewJoinFromSHJ(exec))

    } catch {
      case _ if AuronConf.FORCE_SHUFFLED_HASH_JOIN.booleanConf() =>
        logWarning(
          "in forceShuffledHashJoin mode, hash joins are likely too run OOM because of " +
            "small on-heap memory configuration. to avoid this, we will fall back this " +
            "ShuffledHashJoin to SortMergeJoin. ")

        val leftOrder = leftKeys.map(SortOrder(_, Ascending))
        val rightOrder = rightKeys.map(SortOrder(_, Ascending))
        val leftSorted =
          if (left.outputOrdering.startsWith(leftOrder)) {
            left
          } else {
            val leftSorted = SortExec(leftOrder, global = false, left)
            if (Shims.get.isNative(left)) {
              convertSortExec(leftSorted)
            } else {
              leftSorted
            }
          }
        val rightSorted = if (right.outputOrdering.startsWith(rightOrder)) {
          right
        } else {
          val rightSorted = SortExec(rightOrder, global = false, right)
          if (Shims.get.isNative(right)) {
            convertSortExec(rightSorted)
          } else {
            rightSorted
          }
        }

        val smj =
          SortMergeJoinExec(leftKeys, rightKeys, joinType, condition, leftSorted, rightSorted)
        smj.setTagValue(convertToNonNativeTag, true)
        smj
    }
  }

  def convertBroadcastHashJoinExec(exec: BroadcastHashJoinExec): SparkPlan = {
    try {
      val (leftKeys, rightKeys, joinType, buildSide, condition, left, right) = (
        exec.leftKeys,
        exec.rightKeys,
        exec.joinType,
        exec.buildSide,
        exec.condition,
        exec.left,
        exec.right)
      logDebugPlanConversion(
        exec,
        Seq(
          "leftKeys" -> leftKeys,
          "rightKeys" -> rightKeys,
          "joinType" -> joinType,
          "condition" -> condition,
          "buildSide" -> buildSide))
      assert(condition.isEmpty, "join condition is not supported")

      // verify build side is native
      buildSide match {
        case BuildRight =>
          assert(NativeHelper.isNative(right), "broadcast join build side is not native")
        case BuildLeft =>
          assert(NativeHelper.isNative(left), "broadcast join build side is not native")
      }

      Shims.get.createNativeBroadcastJoinExec(
        addRenameColumnsExec(convertToNative(left)),
        addRenameColumnsExec(convertToNative(right)),
        exec.outputPartitioning,
        leftKeys,
        rightKeys,
        joinType,
        buildSide match {
          case BuildLeft => BroadcastLeft
          case BuildRight => BroadcastRight
        })

    } catch {
      case e @ (_: NotImplementedError | _: Exception) =>
        val underlyingBroadcast = exec.buildSide match {
          case BuildLeft => Shims.get.getUnderlyingBroadcast(exec.left)
          case BuildRight => Shims.get.getUnderlyingBroadcast(exec.right)
        }
        underlyingBroadcast.setTagValue(NativeBroadcastExchangeBase.nativeExecutionTag, false)
        throw e
    }
  }

  def convertBroadcastNestedLoopJoinExec(exec: BroadcastNestedLoopJoinExec): SparkPlan = {
    try {
      val (joinType, buildSide, condition, left, right) =
        (exec.joinType, exec.buildSide, exec.condition, exec.left, exec.right)
      logDebugPlanConversion(
        exec,
        Seq("joinType" -> joinType, "condition" -> condition, "buildSide" -> buildSide))

      assert(condition.isEmpty, "join condition is not supported")

      // verify build side is native
      buildSide match {
        case BuildRight =>
          assert(NativeHelper.isNative(right), "broadcast join build side is not native")
        case BuildLeft =>
          assert(NativeHelper.isNative(left), "broadcast join build side is not native")
      }

      // reuse NativeBroadcastJoin with empty equality keys
      Shims.get.createNativeBroadcastJoinExec(
        addRenameColumnsExec(convertToNative(left)),
        addRenameColumnsExec(convertToNative(right)),
        exec.outputPartitioning,
        Nil,
        Nil,
        joinType,
        buildSide match {
          case BuildLeft => BroadcastLeft
          case BuildRight => BroadcastRight
        })

    } catch {
      case e @ (_: NotImplementedError | _: Exception) =>
        val underlyingBroadcast = exec.buildSide match {
          case BuildLeft => Shims.get.getUnderlyingBroadcast(exec.left)
          case BuildRight => Shims.get.getUnderlyingBroadcast(exec.right)
        }
        underlyingBroadcast.setTagValue(NativeBroadcastExchangeBase.nativeExecutionTag, false)
        throw e
    }
  }

  def convertBroadcastExchangeExec(exec: SparkPlan): SparkPlan = {
    exec match {
      case exec: BroadcastExchangeExec =>
        logDebugPlanConversion(exec, Seq("mode" -> exec.mode))
        val converted = Shims.get.createNativeBroadcastExchangeExec(exec.mode, exec.child)
        converted.setTagValue(NativeBroadcastExchangeBase.nativeExecutionTag, true)
        return converted
    }
    exec
  }

  def convertLocalLimitExec(exec: LocalLimitExec): SparkPlan = {
    logDebugPlanConversion(exec)
    Shims.get.createNativeLocalLimitExec(exec.limit.toLong, exec.child)
  }

  def convertGlobalLimitExec(exec: GlobalLimitExec): SparkPlan = {
    logDebugPlanConversion(exec)
    Shims.get.createNativeGlobalLimitExec(exec.limit.toLong, exec.child)
  }

  def convertTakeOrderedAndProjectExec(exec: TakeOrderedAndProjectExec): SparkPlan = {
    logDebugPlanConversion(exec)
    val nativeTakeOrdered = Shims.get.createNativeTakeOrderedExec(
      exec.limit,
      exec.sortOrder,
      addRenameColumnsExec(convertToNative(exec.child)))

    if (exec.projectList != exec.child.output) {
      val project = ProjectExec(exec.projectList, nativeTakeOrdered)
      tryConvert(project, convertProjectExec)
    } else {
      nativeTakeOrdered
    }
  }

  def convertHashAggregateExec(exec: HashAggregateExec): SparkPlan = {
    // split non-trivial children exprs in partial-agg to a ProjectExec
    // for enabling filter-project optimization in native side
    getPartialAggProjection(exec.aggregateExpressions, exec.groupingExpressions) match {
      case Some((transformedAggregateExprs, transformedGroupingExprs, projections)) =>
        val transformedExec =
          try {
            exec.copy(
              aggregateExpressions = transformedAggregateExprs,
              groupingExpressions = transformedGroupingExprs,
              child = convertProjectExec(ProjectExec(projections, exec.child)))
          } catch {
            case _: NoSuchMethodError =>
              import scala.reflect.runtime.universe._
              import scala.reflect.runtime.currentMirror
              val mirror = currentMirror.reflect(exec)
              val copyMethod = typeOf[HashAggregateExec].decl(TermName("copy")).asMethod
              val params = copyMethod.paramLists.flatten
              val args = params.map { param =>
                param.name.toString match {
                  case "aggregateExpressions" => transformedAggregateExprs
                  case "groupingExpressions" => transformedGroupingExprs
                  case "child" => convertProjectExec(ProjectExec(projections, exec.child))
                  case _ =>
                    mirror
                      .reflectField(
                        typeOf[HashAggregateExec].decl(TermName(param.name.toString)).asTerm)
                      .get
                }
              }
              mirror.reflectMethod(copyMethod)(args: _*).asInstanceOf[HashAggregateExec]
          }
        return convertHashAggregateExec(transformedExec)
      case None => // passthrough
    }

    logDebugPlanConversion(exec)

    // ensure native partial agg exists
    if (exec.requiredChildDistributionExpressions.isDefined) {
      assert(
        NativeAggBase.findPreviousNativeAggrExec(exec).isDefined,
        "partial AggregateExec is not native")
    }
    val nativeAggr = Shims.get.createNativeAggExec(
      NativeAggBase.HashAgg,
      exec.requiredChildDistributionExpressions,
      exec.groupingExpressions,
      exec.aggregateExpressions,
      exec.aggregateAttributes,
      exec.initialInputBufferOffset,
      exec.requiredChildDistributionExpressions match {
        case None =>
          addRenameColumnsExec(convertToNative(exec.child))
        case _ =>
          if (needRenameColumns(exec.child)) {
            val newNames = exec.groupingExpressions.map(Util.getFieldNameByExprId) :+
              NativeAggBase.AGG_BUF_COLUMN_NAME
            Shims.get.createNativeRenameColumnsExec(convertToNative(exec.child), newNames)
          } else {
            convertToNative(exec.child)
          }
      })

    val isFinal = exec.requiredChildDistributionExpressions.isDefined &&
      exec.aggregateExpressions.forall(_.mode == Final)
    if (isFinal) { // wraps with a projection to handle resutExpressions
      try {
        return Shims.get.createNativeProjectExec(exec.resultExpressions, nativeAggr)
      } catch {
        case e @ (_: NotImplementedError | _: AssertionError | _: Exception) =>
          logWarning(
            s"Error projecting resultExpressions, fallback to non-native projection: " +
              s"${e.getMessage}")
          val proj = ProjectExec(exec.resultExpressions, nativeAggr)
          proj.setTagValue(convertToNonNativeTag, true)
          return proj
      }
    }
    nativeAggr
  }

  def convertObjectHashAggregateExec(exec: ObjectHashAggregateExec): SparkPlan = {
    // split non-trivial children exprs in partial-agg to a ProjectExec
    // for enabling filter-project optimization in native side
    getPartialAggProjection(exec.aggregateExpressions, exec.groupingExpressions) match {
      case Some((transformedAggregateExprs, transformedGroupingExprs, projections)) =>
        return convertObjectHashAggregateExec(
          exec.copy(
            aggregateExpressions = transformedAggregateExprs,
            groupingExpressions = transformedGroupingExprs,
            child = convertProjectExec(ProjectExec(projections, exec.child))))
      case None => // passthrough
    }

    logDebugPlanConversion(exec)

    // ensure native partial agg exists
    if (exec.requiredChildDistributionExpressions.isDefined) {
      assert(NativeAggBase.findPreviousNativeAggrExec(exec).isDefined)
    }
    val nativeAggr = Shims.get.createNativeAggExec(
      NativeAggBase.HashAgg,
      exec.requiredChildDistributionExpressions,
      exec.groupingExpressions,
      exec.aggregateExpressions,
      exec.aggregateAttributes,
      exec.initialInputBufferOffset,
      exec.requiredChildDistributionExpressions match {
        case None =>
          addRenameColumnsExec(convertToNative(exec.child))
        case _ =>
          if (needRenameColumns(exec.child)) {
            val newNames = exec.groupingExpressions.map(Util.getFieldNameByExprId) :+
              NativeAggBase.AGG_BUF_COLUMN_NAME
            Shims.get.createNativeRenameColumnsExec(convertToNative(exec.child), newNames)
          } else {
            convertToNative(exec.child)
          }
      })

    val isFinal = exec.requiredChildDistributionExpressions.isDefined &&
      exec.aggregateExpressions.forall(_.mode == Final)
    if (isFinal) { // wraps with a projection to handle resutExpressions
      try {
        return Shims.get.createNativeProjectExec(exec.resultExpressions, nativeAggr)
      } catch {
        case e @ (_: NotImplementedError | _: AssertionError | _: Exception) =>
          logWarning(
            s"Error projecting resultExpressions, fallback to non-native projection: " +
              s"${e.getMessage}")
          val proj = ProjectExec(exec.resultExpressions, nativeAggr)
          proj.setTagValue(convertToNonNativeTag, true)
          return proj
      }
    }
    nativeAggr
  }

  def convertSortAggregateExec(exec: SortAggregateExec): SparkPlan = {
    logDebugPlanConversion(exec)

    // ensure native partial agg exists
    if (exec.requiredChildDistributionExpressions.isDefined) {
      assert(NativeAggBase.findPreviousNativeAggrExec(exec).isDefined)
    }
    val requireOrdering = exec.getTagValue(childOrderingRequiredTag).contains(true)
    val canUseHashAgg = !requireOrdering && (exec.child.isInstanceOf[NativeSortBase] || exec.child
      .isInstanceOf[SortExec])
    val (aggMode, child) = if (canUseHashAgg) {
      (NativeAggBase.HashAgg, exec.child.children.head)
    } else {
      (NativeAggBase.SortAgg, exec.child)
    }

    val nativeAggr = Shims.get.createNativeAggExec(
      aggMode,
      exec.requiredChildDistributionExpressions,
      exec.groupingExpressions,
      exec.aggregateExpressions,
      exec.aggregateAttributes,
      exec.initialInputBufferOffset,
      exec.requiredChildDistributionExpressions match {
        case None =>
          addRenameColumnsExec(convertToNative(child))
        case _ =>
          if (needRenameColumns(child)) {
            val newNames = exec.groupingExpressions.map(Util.getFieldNameByExprId) :+
              NativeAggBase.AGG_BUF_COLUMN_NAME
            Shims.get.createNativeRenameColumnsExec(convertToNative(child), newNames)
          } else {
            convertToNative(child)
          }
      })

    val isFinal = exec.requiredChildDistributionExpressions.isDefined &&
      exec.aggregateExpressions.forall(_.mode == Final)
    if (isFinal) { // wraps with a projection to handle resutExpressions
      try {
        return Shims.get.createNativeProjectExec(exec.resultExpressions, nativeAggr)
      } catch {
        case e @ (_: NotImplementedError | _: AssertionError | _: Exception) =>
          logWarning(
            s"Error projecting resultExpressions, fallback to non-native projection: " +
              s"${e.getMessage}")
          val proj = ProjectExec(exec.resultExpressions, nativeAggr)
          proj.setTagValue(convertToNonNativeTag, true)
          return proj
      }
    }
    nativeAggr
  }

  def convertExpandExec(exec: ExpandExec): SparkPlan = {
    logDebugPlanConversion(exec, Seq("projections" -> exec.projections))
    Shims.get.createNativeExpandExec(
      exec.projections,
      exec.output,
      addRenameColumnsExec(convertToNative(exec.child)))
  }

  def convertWindowExec(exec: WindowExec): SparkPlan = {
    logDebugPlanConversion(
      exec,
      Seq(
        "window exprs" -> exec.windowExpression,
        "partition spec" -> exec.partitionSpec,
        "order spec" -> exec.orderSpec))
    Shims.get.createNativeWindowExec(
      exec.windowExpression,
      exec.partitionSpec,
      exec.orderSpec,
      None,
      outputWindowCols = true,
      addRenameColumnsExec(convertToNative(exec.child)))
  }

  def convertWindowGroupLimitExec(exec: SparkPlan): SparkPlan = {
    // WindowGroupLimit is only supported in Spark3.5+, so use reflection to access its fields
    val (rankLikeFunction, partitionSpec, orderSpec, limit) = (
      MethodUtils.invokeMethod(exec, "rankLikeFunction").asInstanceOf[AggregateWindowFunction],
      MethodUtils.invokeMethod(exec, "partitionSpec").asInstanceOf[Seq[Expression]],
      MethodUtils.invokeMethod(exec, "orderSpec").asInstanceOf[Seq[SortOrder]],
      MethodUtils.invokeMethod(exec, "limit").asInstanceOf[Int])

    logDebugPlanConversion(
      exec,
      Seq(
        "rank like function" -> rankLikeFunction,
        "partition spec" -> partitionSpec,
        "order spec" -> orderSpec,
        "limit" -> limit))

    val windowSpec = WindowSpecDefinition(partitionSpec, orderSpec, rankLikeFunction.frame)
    val windowExpression = WindowExpression(rankLikeFunction, windowSpec)

    Shims.get.createNativeWindowExec(
      Alias(windowExpression, "__window_expression__")() :: Nil,
      partitionSpec,
      orderSpec,
      Some(limit),
      outputWindowCols = false,
      addRenameColumnsExec(convertToNative(exec.children.head)))
  }

  def convertGenerateExec(exec: GenerateExec): SparkPlan = {
    logDebugPlanConversion(
      exec,
      Seq(
        "generator" -> exec.generator,
        "generatorOutput" -> exec.generatorOutput,
        "requiredChildOutput" -> exec.requiredChildOutput,
        "outer" -> exec.outer))
    Shims.get.createNativeGenerateExec(
      exec.generator,
      exec.requiredChildOutput,
      exec.outer,
      exec.generatorOutput,
      addRenameColumnsExec(convertToNative(exec.child)))
  }

  def convertLocalTableScanExec(exec: LocalTableScanExec): SparkPlan = {
    if (exec.rows.isEmpty) {
      return createEmptyExec(exec.output, exec.outputPartitioning, exec.outputOrdering)
    }
    convertToNative(exec)
  }

  def convertDataWritingCommandExec(exec: DataWritingCommandExec): SparkPlan = {
    logDebugPlanConversion(exec)
    exec match {
      case DataWritingCommandExec(cmd: InsertIntoHiveTable, child)
          if cmd.table.storage.outputFormat.contains(
            classOf[MapredParquetOutputFormat].getName) =>
        // add an extra SortExec to sort child with dynamic columns
        // add row number to achieve stable sort
        var sortedChild = convertToNative(child)
        val numDynParts = cmd.partition.count(_._2.isEmpty)
        val requiredOrdering =
          child.output.slice(child.output.length - numDynParts, child.output.length)
        if (requiredOrdering.nonEmpty && child.outputOrdering.map(_.child) != requiredOrdering) {
          val rowNumExpr = StubExpr("RowNum", LongType, nullable = false)
          sortedChild = Shims.get.createNativeSortExec(
            requiredOrdering.map(SortOrder(_, Ascending)) ++ Seq(
              SortOrder(rowNumExpr, Ascending)),
            global = false,
            sortedChild)
        }
        Shims.get.createNativeParquetInsertIntoHiveTableExec(cmd, sortedChild)

      case _ =>
        throw new NotImplementedError("unsupported DataWritingCommandExec")
    }
  }

  def convertToNative(exec: SparkPlan): SparkPlan = {
    exec match {
      case exec if NativeHelper.isNative(exec) => exec
      case exec =>
        assert(exec.find(_.isInstanceOf[DataWritingCommandExec]).isEmpty)
        Shims.get.createConvertToNativeExec(exec)
    }
  }

  def createEmptyExec(
      output: Seq[Attribute],
      outputPartitioning: Partitioning,
      outputOrdering: Seq[SortOrder]): SparkPlan = {

    case class NativeEmptyExec(
        override val output: Seq[Attribute],
        override val outputPartitioning: Partitioning,
        override val outputOrdering: Seq[SortOrder])
        extends LeafExecNode
        with NativeSupports {

      override protected def doExecuteNative(): NativeRDD = {
        val nativeSchema = Util.getNativeSchema(output)
        val partitions = Range(0, outputPartitioning.numPartitions)
          .map(i =>
            new Partition {
              override def index: Int = i
            })
          .toArray

        new NativeRDD(
          sparkContext,
          MetricNode(Map(), Nil),
          rddPartitions = partitions,
          rddPartitioner = None,
          rddDependencies = Nil,
          false,
          (_partition, _taskContext) => {
            val nativeEmptyExec = EmptyPartitionsExecNode
              .newBuilder()
              .setNumPartitions(outputPartitioning.numPartitions)
              .setSchema(nativeSchema)
            PhysicalPlanNode.newBuilder().setEmptyPartitions(nativeEmptyExec).build()
          },
          friendlyName = "NativeRDD.Empty")
      }
    }
    NativeEmptyExec(output, outputPartitioning, outputOrdering)
  }

  @tailrec
  def needRenameColumns(plan: SparkPlan): Boolean = {
    if (plan.output.isEmpty) {
      return false
    }
    plan match {
      case _: NativeParquetScanBase | _: NativeOrcScanBase | _: NativeHiveTableScanBase |
          _: NativeUnionBase =>
        true
      case _: ConvertToNativeBase => needRenameColumns(plan.children.head)
      case exec if NativeHelper.isNative(exec) =>
        NativeHelper.getUnderlyingNativePlan(exec).output != plan.output
      case _ => false
    }
  }

  def addRenameColumnsExec(exec: SparkPlan): SparkPlan = {
    if (needRenameColumns(exec)) {
      return Shims.get.createNativeRenameColumnsExec(
        exec,
        exec.output.map(Util.getFieldNameByExprId))
    }
    exec
  }

  private def getPartialAggProjection(
      aggregateExprs: Seq[AggregateExpression],
      groupingExprs: Seq[NamedExpression])
      : Option[(Seq[AggregateExpression], Seq[NamedExpression], Seq[Alias])] = {

    if (!aggregateExprs.forall(_.mode == Partial)) {
      return None
    }
    val aggExprChildren = aggregateExprs.flatMap(_.aggregateFunction.children)
    val containsNonTrivial = (aggExprChildren ++ groupingExprs).exists {
      case _: AttributeReference | _: Literal => false
      case e: Alias if e.child.isInstanceOf[Literal] => false
      case _ => true
    }
    if (!containsNonTrivial) {
      return None
    }

    val projections = mutable.LinkedHashMap[Expression, AttributeReference]()
    val transformedGroupingExprs = groupingExprs.map {
      case e @ Alias(_: Literal, _) => e
      case e =>
        projections.getOrElseUpdate(
          e,
          AttributeReference(e.name, e.dataType, e.nullable, e.metadata)(e.exprId, e.qualifier))
    }
    val transformedAggExprs = aggregateExprs.map { expr =>
      expr.copy(aggregateFunction = expr.aggregateFunction
        .mapChildren {
          case e @ (Literal(_, _) | Alias(_: Literal, _)) => e
          case e =>
            val nextCol = s"_c${projections.size}"
            projections.getOrElseUpdate(e, AttributeReference(nextCol, e.dataType, e.nullable)())
        }
        .asInstanceOf[AggregateFunction])
    }
    Some(
      (
        transformedAggExprs.toList,
        transformedGroupingExprs.toList,
        projections.map(kv => Alias(kv._1, kv._2.name)(kv._2.exprId)).toList))
  }

  def getBooleanConf(key: String, defaultValue: Boolean): Boolean = {
    val s = SQLConf.get.getConfString(key, defaultValue.toString)
    try {
      s.trim.toBoolean
    } catch {
      case _: IllegalArgumentException =>
        throw new IllegalArgumentException(s"$key should be boolean, but was $s")
    }
  }

  abstract class ForceNativeExecutionWrapperBase(override val child: SparkPlan)
      extends UnaryExecNode
      with NativeSupports {

    setTagValue(convertibleTag, true)
    setTagValue(convertStrategyTag, AlwaysConvert)

    override def output: Seq[Attribute] = child.output
    override def outputPartitioning: Partitioning = child.outputPartitioning
    override def outputOrdering: Seq[SortOrder] = child.outputOrdering
    override def requiredChildOrdering: Seq[Seq[SortOrder]] = child.requiredChildOrdering
    override def doExecuteNative(): NativeRDD = Shims.get.executeNative(child)
    override def doExecuteBroadcast[T](): Broadcast[T] = child.doExecuteBroadcast()

    override val nodeName: String = "InputAdapter"
  }
}
