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

import scala.annotation.tailrec
import scala.collection.mutable

import org.apache.commons.lang3.reflect.MethodUtils
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.blaze.BlazeConvertStrategy.childOrderingRequiredTag
import org.apache.spark.sql.blaze.BlazeConvertStrategy.convertibleTag
import org.apache.spark.sql.blaze.BlazeConvertStrategy.convertStrategyTag
import org.apache.spark.sql.blaze.BlazeConvertStrategy.convertToNonNativeTag
import org.apache.spark.sql.blaze.BlazeConvertStrategy.isNeverConvert
import org.apache.spark.sql.blaze.BlazeConvertStrategy.joinSmallerSideTag
import org.apache.spark.sql.blaze.NativeConverters.{roundRobinTypeSupported, scalarTypeSupported, StubExpr}
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.Final
import org.apache.spark.sql.catalyst.expressions.aggregate.Partial
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.catalyst.plans.physical.RoundRobinPartitioning
import org.apache.spark.sql.catalyst.plans.physical.RangePartitioning
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.FilterExec
import org.apache.spark.sql.execution.GlobalLimitExec
import org.apache.spark.sql.execution.LocalLimitExec
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.execution.SortExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.TakeOrderedAndProjectExec
import org.apache.spark.sql.execution.UnionExec
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.aggregate.ObjectHashAggregateExec
import org.apache.spark.sql.execution.blaze.plan.NativeAggBase
import org.apache.spark.sql.execution.blaze.plan.NativeUnionBase
import org.apache.spark.sql.execution.blaze.plan.Util
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.execution.ExpandExec
import org.apache.spark.sql.execution.aggregate.SortAggregateExec
import org.apache.spark.sql.execution.blaze.plan.NativeBroadcastExchangeBase
import org.apache.spark.sql.execution.GenerateExec
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.blaze.plan.BroadcastLeft
import org.apache.spark.sql.execution.blaze.plan.BroadcastRight
import org.apache.spark.sql.execution.blaze.plan.ConvertToNativeBase
import org.apache.spark.sql.execution.blaze.plan.NativeOrcScanBase
import org.apache.spark.sql.execution.blaze.plan.NativeParquetScanBase
import org.apache.spark.sql.execution.blaze.plan.NativeSortBase
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.hive.blaze.BlazeHiveConverters
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable
import org.apache.spark.sql.hive.execution.blaze.plan.NativeHiveTableScanBase
import org.apache.spark.sql.types.LongType
import org.apache.spark.Partition
import org.apache.spark.sql.catalyst.expressions.AggregateWindowFunction
import org.blaze.protobuf.EmptyPartitionsExecNode
import org.blaze.protobuf.PhysicalPlanNode
import org.apache.spark.sql.catalyst.expressions.WindowExpression
import org.apache.spark.sql.catalyst.expressions.WindowSpecDefinition
import org.apache.spark.sql.internal.SQLConf
import org.blaze.sparkver

object BlazeConverters extends Logging {
  def enableScan: Boolean =
    getBooleanConf("spark.blaze.enable.scan", defaultValue = true)
  def enablePaimonScan: Boolean =
    getBooleanConf("spark.blaze.enable.paimon.scan", defaultValue = false)
  def enableProject: Boolean =
    getBooleanConf("spark.blaze.enable.project", defaultValue = true)
  def enableFilter: Boolean =
    getBooleanConf("spark.blaze.enable.filter", defaultValue = true)
  def enableSort: Boolean =
    getBooleanConf("spark.blaze.enable.sort", defaultValue = true)
  def enableUnion: Boolean =
    getBooleanConf("spark.blaze.enable.union", defaultValue = true)
  def enableSmj: Boolean =
    getBooleanConf("spark.blaze.enable.smj", defaultValue = true)
  def enableShj: Boolean =
    getBooleanConf("spark.blaze.enable.shj", defaultValue = true)
  def enableBhj: Boolean =
    getBooleanConf("spark.blaze.enable.bhj", defaultValue = true)
  def enableBnlj: Boolean =
    getBooleanConf("spark.blaze.enable.bnlj", defaultValue = true)
  def enableLocalLimit: Boolean =
    getBooleanConf("spark.blaze.enable.local.limit", defaultValue = true)
  def enableGlobalLimit: Boolean =
    getBooleanConf("spark.blaze.enable.global.limit", defaultValue = true)
  def enableTakeOrderedAndProject: Boolean =
    getBooleanConf("spark.blaze.enable.take.ordered.and.project", defaultValue = true)
  def enableAggr: Boolean =
    getBooleanConf("spark.blaze.enable.aggr", defaultValue = true)
  def enableExpand: Boolean =
    getBooleanConf("spark.blaze.enable.expand", defaultValue = true)
  def enableWindow: Boolean =
    getBooleanConf("spark.blaze.enable.window", defaultValue = true)
  def enableWindowGroupLimit: Boolean =
    getBooleanConf("spark.blaze.enable.window.group.limit", defaultValue = true)
  def enableGenerate: Boolean =
    getBooleanConf("spark.blaze.enable.generate", defaultValue = true)
  def enableLocalTableScan: Boolean =
    getBooleanConf("spark.blaze.enable.local.table.scan", defaultValue = true)
  def enableDataWriting: Boolean =
    getBooleanConf("spark.blaze.enable.data.writing", defaultValue = false)
  def enableScanParquet: Boolean =
    getBooleanConf("spark.blaze.enable.scan.parquet", defaultValue = true)
  def enableScanOrc: Boolean =
    getBooleanConf("spark.blaze.enable.scan.orc", defaultValue = true)

  import org.apache.spark.sql.catalyst.plans._
  import org.apache.spark.sql.catalyst.optimizer._
  var _UnusedQueryPlan: QueryPlan[_] = _
  var _UnusedOptimizer: Optimizer = _

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
      case e
          if enablePaimonScan && BlazeHiveConverters.isNativePaimonTableScan(e) => // scan paimon
        tryConvert(e, BlazeHiveConverters.convertPaimonTableScanExec)
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
    logDebug(s"Converting ShuffleExchangeExec: ${Shims.get.simpleStringWithNodeId(exec)}")

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
    logDebug(s"Converting FileSourceScanExec: ${Shims.get.simpleStringWithNodeId(exec)}")
    logDebug(s"  relation: ${relation}")
    logDebug(s"  relation.location: ${relation.location}")
    logDebug(s"  output: ${output}")
    logDebug(s"  requiredSchema: ${requiredSchema}")
    logDebug(s"  partitionFilters: ${partitionFilters}")
    logDebug(s"  optionalBucketSet: ${optionalBucketSet}")
    logDebug(s"  dataFilters: ${dataFilters}")
    logDebug(s"  tableIdentifier: ${tableIdentifier}")
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
      logDebug(s"Converting ProjectExec: ${Shims.get.simpleStringWithNodeId(exec)}")
      projectList.foreach(p => logDebug(s"  projectExpr: ${p}"))
    }
    Shims.get.createNativeProjectExec(projectList, addRenameColumnsExec(convertToNative(child)))
  }

  def convertFilterExec(exec: FilterExec): SparkPlan =
    exec match {
      case exec: FilterExec =>
        logDebug(s"Converting FilterExec: ${Shims.get.simpleStringWithNodeId(exec)}")
        logDebug(s"  condition: ${exec.condition}")
        Shims.get.createNativeFilterExec(
          exec.condition,
          addRenameColumnsExec(convertToNative(exec.child)))
      case _ =>
        logDebug(s"Ignoring FilterExec: ${Shims.get.simpleStringWithNodeId(exec)}")
        exec
    }

  def convertSortExec(exec: SortExec): SparkPlan = {
    val (sortOrder, global, child) = (exec.sortOrder, exec.global, exec.child)
    logDebug(s"Converting SortExec: ${Shims.get.simpleStringWithNodeId(exec)}")
    logDebug(s"  global: ${global}")
    sortOrder.foreach(s => logDebug(s"  sortOrder: ${s}"))
    Shims.get.createNativeSortExec(
      sortOrder,
      global,
      addRenameColumnsExec(convertToNative(child)))
  }

  def convertUnionExec(exec: UnionExec): SparkPlan = {
    logDebug(s"Converting UnionExec: ${Shims.get.simpleStringWithNodeId(exec)}")
    Shims.get.createNativeUnionExec(
      exec.children.map(child => addRenameColumnsExec(convertToNative(child))),
      exec.output)
  }

  def convertSortMergeJoinExec(exec: SortMergeJoinExec): SparkPlan = {
    val requireOrdering = exec.getTagValue(childOrderingRequiredTag).contains(true)

    // force shuffled-hash join
    if (!requireOrdering
      && BlazeConf.FORCE_SHUFFLED_HASH_JOIN.booleanConf()
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
      logDebug(
        s"Converting SortMergeJoinExec (with forceShuffledHashJoin): ${Shims.get.simpleStringWithNodeId(exec)}")
      logDebug(s"  leftKeys: $leftKeys")
      logDebug(s"  rightKeys: $rightKeys")
      logDebug(s"  joinType: $joinType")
      logDebug(s"  condition: $condition")
      assert(condition.isEmpty, "join condition is not supported")

      val buildSide = exec.getTagValue(joinSmallerSideTag) match {
        case Some(org.apache.spark.sql.execution.blaze.plan.BuildLeft) =>
          org.apache.spark.sql.execution.blaze.plan.BuildLeft
        case Some(org.apache.spark.sql.execution.blaze.plan.BuildRight) =>
          org.apache.spark.sql.execution.blaze.plan.BuildRight
        case None =>
          logWarning("JoinSmallerSideTag is missing, defaults to BuildRight")
          org.apache.spark.sql.execution.blaze.plan.BuildRight
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
    logDebug(s"Converting SortMergeJoinExec: ${Shims.get.simpleStringWithNodeId(exec)}")
    logDebug(s"  leftKeys: $leftKeys")
    logDebug(s"  rightKeys: $rightKeys")
    logDebug(s"  joinType: $joinType")
    logDebug(s"  condition: $condition")
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
    logDebug(s"Converting ShuffledHashJoinExec: ${Shims.get.simpleStringWithNodeId(exec)}")
    logDebug(s"  leftKeys: $leftKeys")
    logDebug(s"  rightKeys: $rightKeys")
    logDebug(s"  joinType: $joinType")
    logDebug(s"  condition: $condition")
    logDebug(s"  buildSide: $buildSide")

    try {
      assert(condition.isEmpty, "join condition is not supported")
      Shims.get.createNativeShuffledHashJoinExec(
        addRenameColumnsExec(convertToNative(left)),
        addRenameColumnsExec(convertToNative(right)),
        leftKeys,
        rightKeys,
        joinType,
        buildSide match {
          case BuildLeft => org.apache.spark.sql.execution.blaze.plan.BuildLeft
          case BuildRight => org.apache.spark.sql.execution.blaze.plan.BuildRight
        },
        getIsSkewJoinFromSHJ(exec))

    } catch {
      case _ if BlazeConf.FORCE_SHUFFLED_HASH_JOIN.booleanConf() =>
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
      logDebug(s"Converting BroadcastHashJoinExec: ${Shims.get.simpleStringWithNodeId(exec)}")
      logDebug(s"  leftKeys: $leftKeys")
      logDebug(s"  rightKeys: $rightKeys")
      logDebug(s"  joinType: $joinType")
      logDebug(s"  buildSide: $buildSide")
      logDebug(s"  condition: $condition")
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
      logDebug(
        s"Converting BroadcastNestedLoopJoinExec: ${Shims.get.simpleStringWithNodeId(exec)}")
      logDebug(s"  joinType: ${exec.joinType}")
      logDebug(s"  buildSide: ${exec.buildSide}")
      logDebug(s"  condition: ${exec.condition}")
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
        logDebug(s"Converting BroadcastExchangeExec: ${Shims.get.simpleStringWithNodeId(exec)}")
        logDebug(s"  mode: ${exec.mode}")
        val converted = Shims.get.createNativeBroadcastExchangeExec(exec.mode, exec.child)
        converted.setTagValue(NativeBroadcastExchangeBase.nativeExecutionTag, true)
        return converted
    }
    exec
  }

  def convertLocalLimitExec(exec: LocalLimitExec): SparkPlan = {
    logDebug(s"Converting LocalLimitExec: ${Shims.get.simpleStringWithNodeId(exec)}")
    Shims.get.createNativeLocalLimitExec(exec.limit.toLong, exec.child)
  }

  def convertGlobalLimitExec(exec: GlobalLimitExec): SparkPlan = {
    logDebug(s"Converting GlobalLimitExec: ${Shims.get.simpleStringWithNodeId(exec)}")
    Shims.get.createNativeGlobalLimitExec(exec.limit.toLong, exec.child)
  }

  def convertTakeOrderedAndProjectExec(exec: TakeOrderedAndProjectExec): SparkPlan = {
    logDebug(s"Converting TakeOrderedAndProjectExec: ${Shims.get.simpleStringWithNodeId(exec)}")
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

    logDebug(s"Converting HashAggregateExec: ${Shims.get.simpleStringWithNodeId(exec)}")

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

    logDebug(s"Converting ObjectHashAggregateExec: ${Shims.get.simpleStringWithNodeId(exec)}")

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
    logDebug(s"Converting SortAggregateExec: ${Shims.get.simpleStringWithNodeId(exec)}")

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
    logDebug(s"Converting ExpandExec: ${Shims.get.simpleStringWithNodeId(exec)}")
    logDebug(s"  projections: ${exec.projections}")
    Shims.get.createNativeExpandExec(
      exec.projections,
      exec.output,
      addRenameColumnsExec(convertToNative(exec.child)))
  }

  def convertWindowExec(exec: WindowExec): SparkPlan = {
    logDebug(s"Converting WindowExec: ${Shims.get.simpleStringWithNodeId(exec)}")
    logDebug(s"  window exprs: ${exec.windowExpression}")
    logDebug(s"  partition spec: ${exec.partitionSpec}")
    logDebug(s"  order spec: ${exec.orderSpec}")
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

    logDebug(s"Converting WindowGroupLimitExec: ${Shims.get.simpleStringWithNodeId(exec)}")
    logDebug(s"  rank like function: $rankLikeFunction")
    logDebug(s"  partition spec: $partitionSpec")
    logDebug(s"  order spec: $orderSpec")
    logDebug(s"  limit: $limit")

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
    logDebug(s"Converting GenerateExec: ${Shims.get.simpleStringWithNodeId(exec)}")
    logDebug(s"  generator: ${exec.generator}")
    logDebug(s"  generatorOutput: ${exec.generatorOutput}")
    logDebug(s"  requiredChildOutput: ${exec.requiredChildOutput}")
    logDebug(s"  outer: ${exec.outer}")
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
    logDebug(s"Converting DataWritingCommandExec: ${Shims.get.simpleStringWithNodeId(exec)}")
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
          (_, _) => {
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

  private def getBooleanConf(key: String, defaultValue: Boolean): Boolean = {
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
