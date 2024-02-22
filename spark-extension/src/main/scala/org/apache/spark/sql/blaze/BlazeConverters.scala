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

import java.util.UUID

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat
import org.apache.spark.SparkEnv
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.blaze.BlazeConvertStrategy.convertibleTag
import org.apache.spark.sql.blaze.BlazeConvertStrategy.convertStrategyTag
import org.apache.spark.sql.blaze.BlazeConvertStrategy.isNeverConvert
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
import org.apache.spark.sql.execution.blaze.plan._
import org.apache.spark.sql.execution.blaze.plan.NativeAggBase
import org.apache.spark.sql.execution.blaze.plan.NativeProjectBase
import org.apache.spark.sql.execution.blaze.plan.NativeUnionBase
import org.apache.spark.sql.execution.blaze.plan.Util
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
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
import org.apache.spark.sql.execution.blaze.plan.NativeParquetScanBase
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable

object BlazeConverters extends Logging {
  val enableScan: Boolean =
    SparkEnv.get.conf.getBoolean("spark.blaze.enable.scan", defaultValue = true)
  val enableProject: Boolean =
    SparkEnv.get.conf.getBoolean("spark.blaze.enable.project", defaultValue = true)
  val enableFilter: Boolean =
    SparkEnv.get.conf.getBoolean("spark.blaze.enable.filter", defaultValue = true)
  val enableSort: Boolean =
    SparkEnv.get.conf.getBoolean("spark.blaze.enable.sort", defaultValue = true)
  val enableUnion: Boolean =
    SparkEnv.get.conf.getBoolean("spark.blaze.enable.union", defaultValue = true)
  val enableSmj: Boolean =
    SparkEnv.get.conf.getBoolean("spark.blaze.enable.smj", defaultValue = true)
  val enableBhj: Boolean =
    SparkEnv.get.conf.getBoolean("spark.blaze.enable.bhj", defaultValue = true)
  val enableBnlj: Boolean =
    SparkEnv.get.conf.getBoolean("spark.blaze.enable.bnlj", defaultValue = true)
  val enableLocalLimit: Boolean =
    SparkEnv.get.conf.getBoolean("spark.blaze.enable.local.limit", defaultValue = true)
  val enableGlobalLimit: Boolean =
    SparkEnv.get.conf.getBoolean("spark.blaze.enable.global.limit", defaultValue = true)
  val enableTakeOrderedAndProject: Boolean =
    SparkEnv.get.conf
      .getBoolean("spark.blaze.enable.take.ordered.and.project", defaultValue = true)
  val enableAggr: Boolean =
    SparkEnv.get.conf.getBoolean("spark.blaze.enable.aggr", defaultValue = true)
  val enableExpand: Boolean =
    SparkEnv.get.conf.getBoolean("spark.blaze.enable.expand", defaultValue = true)
  val enableWindow: Boolean =
    SparkEnv.get.conf.getBoolean("spark.blaze.enable.window", defaultValue = true)
  val enableGenerate: Boolean =
    SparkEnv.get.conf.getBoolean("spark.blaze.enable.generate", defaultValue = true)
  val enableLocalTableScan: Boolean =
    SparkEnv.get.conf.getBoolean("spark.blaze.enable.local.table.scan", defaultValue = true)
  val enableDataWriting: Boolean =
    SparkEnv.get.conf.getBoolean("spark.blaze.enable.data.writing", defaultValue = false)

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
      if (!isNeverConvert(exec)) {
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
        logWarning(s"Error converting exec: ${exec.getClass.getSimpleName}: ${e.getMessage}", e)
        exec.setTagValue(convertibleTag, false)
        exec.setTagValue(convertStrategyTag, NeverConvert)
        exec
    }
  }

  def convertShuffleExchangeExec(exec: ShuffleExchangeExec): SparkPlan = {
    val (outputPartitioning, child) = (exec.outputPartitioning, exec.child)
    logDebug(s"Converting ShuffleExchangeExec: ${Shims.get.simpleStringWithNodeId(exec)}")

    assert(
      exec.outputPartitioning.numPartitions == 1 || exec.outputPartitioning
        .isInstanceOf[HashPartitioning])

    val convertedChild = outputPartitioning match {
      case p if p.isInstanceOf[HashPartitioning] || p.numPartitions == 1 =>
        convertToNative(child)
      case _ => child
    }
    Shims.get.createNativeShuffleExchangeExec(
      outputPartitioning,
      addRenameColumnsExec(convertedChild))
  }

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
    assert(
      relation.fileFormat.isInstanceOf[ParquetFileFormat],
      "Cannot convert non-parquet scan exec")
    logDebug(s"Converting FileSourceScanExec: ${Shims.get.simpleStringWithNodeId(exec)}")
    logDebug(s"  relation: ${relation}")
    logDebug(s"  relation.location: ${relation.location}")
    logDebug(s"  output: ${output}")
    logDebug(s"  requiredSchema: ${requiredSchema}")
    logDebug(s"  partitionFilters: ${partitionFilters}")
    logDebug(s"  optionalBucketSet: ${optionalBucketSet}")
    logDebug(s"  dataFilters: ${dataFilters}")
    logDebug(s"  tableIdentifier: ${tableIdentifier}")
    addRenameColumnsExec(Shims.get.createNativeParquetScanExec(exec))
  }

  def convertProjectExec(exec: ProjectExec): SparkPlan = {
    val (projectList, child) = (exec.projectList, exec.child)
    logDebug(s"Converting ProjectExec: ${Shims.get.simpleStringWithNodeId(exec)}")
    projectList.foreach(p => logDebug(s"  projectExpr: ${p}"))
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
    Shims.get.createNativeUnionExec(exec.children.map(child => {
      addRenameColumnsExec(convertToNative(child))
    }))
  }

  def convertSortMergeJoinExec(exec: SortMergeJoinExec): SparkPlan = {
    val (leftKeys, rightKeys, joinType, condition, left, right) =
      (exec.leftKeys, exec.rightKeys, exec.joinType, exec.condition, exec.left, exec.right)
    logDebug(s"Converting SortMergeJoinExec: ${Shims.get.simpleStringWithNodeId(exec)}")
    var nativeLeft = convertToNative(left)
    var nativeRight = convertToNative(right)
    var modifiedLeftKeys = leftKeys
    var modifiedRightKeys = rightKeys
    var needPostProject = false

    if (leftKeys.exists(!_.isInstanceOf[AttributeReference])) {
      val (keys, exec) = buildJoinColumnsProject(nativeLeft, leftKeys)
      modifiedLeftKeys = keys
      nativeLeft = exec
      needPostProject = true
    }
    if (rightKeys.exists(!_.isInstanceOf[AttributeReference])) {
      val (keys, exec) = buildJoinColumnsProject(nativeRight, rightKeys)
      modifiedRightKeys = keys
      nativeRight = exec
      needPostProject = true
    }

    val smjOrig = SortMergeJoinExec(
      modifiedLeftKeys,
      modifiedRightKeys,
      joinType,
      condition,
      addRenameColumnsExec(nativeLeft),
      addRenameColumnsExec(nativeRight))
    val smj = Shims.get.createNativeSortMergeJoinExec(
      smjOrig.left,
      smjOrig.right,
      smjOrig.leftKeys,
      smjOrig.rightKeys,
      smjOrig.joinType,
      smjOrig.condition)

    if (needPostProject) {
      buildPostJoinProject(smj, exec.output)
    } else {
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
      logDebug(s"  leftKeys: ${exec.leftKeys}")
      logDebug(s"  rightKeys: ${exec.rightKeys}")
      logDebug(s"  joinType: ${exec.joinType}")
      logDebug(s"  buildSide: ${exec.buildSide}")
      logDebug(s"  condition: ${exec.condition}")
      var (hashed, hashedKeys, nativeProbed, probedKeys) = buildSide match {
        case BuildRight =>
          assert(NativeHelper.isNative(right), "broadcast join build side is not native")
          val convertedLeft = convertToNative(left)
          (right, rightKeys, convertedLeft, leftKeys)

        case BuildLeft =>
          assert(NativeHelper.isNative(left), "broadcast join build side is not native")
          val convertedRight = convertToNative(right)
          (left, leftKeys, convertedRight, rightKeys)

        case _ =>
          // scalastyle:off throwerror
          throw new NotImplementedError(
            "Ignore BroadcastHashJoin with unsupported children structure")
      }

      var modifiedHashedKeys = hashedKeys
      var modifiedProbedKeys = probedKeys
      var needPostProject = false

      if (hashedKeys.exists(!_.isInstanceOf[AttributeReference])) {
        val (keys, exec) = buildJoinColumnsProject(hashed, hashedKeys)
        modifiedHashedKeys = keys
        hashed = exec
        needPostProject = true
      }
      if (probedKeys.exists(!_.isInstanceOf[AttributeReference])) {
        val (keys, exec) = buildJoinColumnsProject(nativeProbed, probedKeys)
        modifiedProbedKeys = keys
        nativeProbed = exec
        needPostProject = true
      }

      val modifiedJoinType = buildSide match {
        case BuildLeft => joinType
        case BuildRight =>
          needPostProject = true
          val modifiedJoinType = joinType match { // reverse join type
            case Inner => Inner
            case FullOuter => FullOuter
            case LeftOuter => RightOuter
            case RightOuter => LeftOuter
            case _ =>
              throw new NotImplementedError(
                "BHJ Semi/Anti join with BuildRight is not yet supported")
          }
          modifiedJoinType
      }

      val bhjOrig = BroadcastHashJoinExec(
        modifiedHashedKeys,
        modifiedProbedKeys,
        modifiedJoinType,
        BuildLeft,
        condition,
        addRenameColumnsExec(hashed),
        addRenameColumnsExec(nativeProbed))

      val bhj = Shims.get.createNativeBroadcastJoinExec(
        bhjOrig.left,
        bhjOrig.right,
        bhjOrig.outputPartitioning,
        bhjOrig.leftKeys,
        bhjOrig.rightKeys,
        bhjOrig.joinType,
        bhjOrig.condition)

      if (needPostProject) {
        buildPostJoinProject(bhj, exec.output)
      } else {
        bhj
      }
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
      val (broadcasted, nativeProbed) = buildSide match {
        case BuildRight =>
          assert(NativeHelper.isNative(right), "broadcast join build side is not native")
          val convertedLeft = convertToNative(left)
          (right, convertedLeft)

        case BuildLeft =>
          assert(NativeHelper.isNative(left), "broadcast join build side is not native")
          val convertedRight = convertToNative(right)
          (left, convertedRight)

        case _ =>
          // scalastyle:off throwerror
          throw new NotImplementedError(
            "Ignore BroadcastNestedLoopJoin with unsupported children structure")
      }

      // the in-memory inner table is not the same in different join types
      // reference: https://docs.rs/datafusion/latest/datafusion/physical_plan/joins/struct.NestedLoopJoinExec.html
      var needPostProject = false
      val (modifiedLeft, modifiedRight, modifiedJoinType) = (buildSide, joinType) match {
        case (BuildLeft, RightOuter | FullOuter) => (broadcasted, nativeProbed, joinType)
        case (BuildRight, Inner | LeftOuter | LeftSemi | LeftAnti) =>
          (nativeProbed, broadcasted, joinType)
        case _ =>
          needPostProject = true
          val modifiedJoinType = joinType match { // reverse join type
            case Inner => (nativeProbed, broadcasted, Inner)
            case FullOuter => (broadcasted, nativeProbed, FullOuter)
            case LeftOuter => (broadcasted, nativeProbed, RightOuter)
            case RightOuter => (nativeProbed, broadcasted, LeftOuter)
            case _ =>
              throw new NotImplementedError(
                s"BNLJ $joinType with $buildSide is not yet supported")
          }
          modifiedJoinType
      }

      val bnlj = Shims.get.createNativeBroadcastNestedLoopJoinExec(
        addRenameColumnsExec(modifiedLeft),
        addRenameColumnsExec(modifiedRight),
        modifiedJoinType,
        condition)

      if (needPostProject) {
        buildPostJoinProject(bnlj, exec.output)
      } else {
        bnlj
      }
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
        return convertHashAggregateExec(
          exec.copy(
            aggregateExpressions = transformedAggregateExprs,
            groupingExpressions = transformedGroupingExprs,
            child = convertProjectExec(ProjectExec(projections, exec.child))))
      case None => // passthrough
    }

    logDebug(s"Converting HashAggregateExec: ${Shims.get.simpleStringWithNodeId(exec)}")

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
            s"Error projecting resultExpressions, failback to non-native projection: " +
              s"${e.getMessage}")
          val proj = ProjectExec(exec.resultExpressions, nativeAggr)
          proj.setTagValue(convertibleTag, true)
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
            s"Error projecting resultExpressions, failback to non-native projection: " +
              s"${e.getMessage}")
          val proj = ProjectExec(exec.resultExpressions, nativeAggr)
          proj.setTagValue(convertibleTag, true)
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
    val nativeAggr = Shims.get.createNativeAggExec(
      NativeAggBase.SortAgg,
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
            s"Error projecting resultExpressions, failback to non-native projection: " +
              s"${e.getMessage}")
          val proj = ProjectExec(exec.resultExpressions, nativeAggr)
          proj.setTagValue(convertibleTag, true)
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
      addRenameColumnsExec(convertToNative(exec.child)))
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
    convertToNative(exec)
  }

  def convertDataWritingCommandExec(exec: DataWritingCommandExec): SparkPlan = {
    logDebug(s"Converting DataWritingCommandExec: ${Shims.get.simpleStringWithNodeId(exec)}")
    exec match {
      case DataWritingCommandExec(cmd: InsertIntoHiveTable, child)
          if cmd.table.storage.outputFormat.contains(
            classOf[MapredParquetOutputFormat].getName) =>
        // add an extra SortExec to sort child with dynamic columns
        var sortedChild = convertToNative(child)
        val numDynParts = cmd.partition.count(_._2.isEmpty)
        val requiredOrdering =
          child.output.slice(child.output.length - numDynParts, child.output.length)
        if (child.outputOrdering.map(_.child) != requiredOrdering) {
          sortedChild = Shims.get.createNativeSortExec(
            requiredOrdering.map(SortOrder(_, Ascending)),
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

  @tailrec
  def needRenameColumns(plan: SparkPlan): Boolean = {
    if (plan.output.isEmpty) {
      return false
    }
    plan match {
      case _: NativeParquetScanBase | _: NativeUnionBase => true
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

  private def buildJoinColumnsProject(
      child: SparkPlan,
      joinKeys: Seq[Expression]): (Seq[AttributeReference], NativeProjectBase) = {
    val extraProjectList = ArrayBuffer[NamedExpression]()
    val transformedKeys = ArrayBuffer[AttributeReference]()

    joinKeys.foreach {
      case attr: AttributeReference => transformedKeys.append(attr)
      case expr =>
        val aliasExpr =
          Alias(expr, s"JOIN_KEY:${expr.toString()} (${UUID.randomUUID().toString})")()
        extraProjectList.append(aliasExpr)

        val attr = AttributeReference(
          aliasExpr.name,
          aliasExpr.dataType,
          aliasExpr.nullable,
          aliasExpr.metadata)(aliasExpr.exprId, aliasExpr.qualifier)
        transformedKeys.append(attr)
    }
    (
      transformedKeys,
      Shims.get
        .createNativeProjectExec(child.output ++ extraProjectList, addRenameColumnsExec(child)))
  }

  private def buildPostJoinProject(
      child: SparkPlan,
      output: Seq[Attribute]): NativeProjectBase = {
    val projectList = output
      .filter(!_.name.startsWith("JOIN_KEY:"))
      .map(attr =>
        AttributeReference(attr.name, attr.dataType, attr.nullable, attr.metadata)(
          attr.exprId,
          attr.qualifier))
    Shims.get.createNativeProjectExec(projectList, child)
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
