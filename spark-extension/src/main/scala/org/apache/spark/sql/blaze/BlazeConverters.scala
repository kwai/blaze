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
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.blaze.BlazeConvertStrategy.convertibleTag
import org.apache.spark.sql.blaze.BlazeConvertStrategy.convertStrategyTag
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.catalyst.plans.FullOuter
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.catalyst.plans.RightOuter
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec}
import org.apache.spark.sql.execution.blaze.plan.NativeProjectExec
import org.apache.spark.sql.execution.joins.{
  BroadcastHashJoinExec,
  BuildLeft,
  BuildRight,
  CartesianProductExec,
  SortMergeJoinExec
}
import org.apache.spark.sql.execution.{
  CollectLimitExec,
  FileSourceScanExec,
  FilterExec,
  GlobalLimitExec,
  LocalLimitExec,
  ProjectExec,
  SortExec,
  SparkPlan,
  TakeOrderedAndProjectExec,
  UnaryExecNode,
  UnionExec
}
import org.apache.spark.sql.execution.blaze.plan.ConvertToNativeExec
import org.apache.spark.sql.execution.blaze.plan.NativeHashAggregateExec
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.blaze.plan.ConvertToUnsafeRowExec
import org.apache.spark.sql.execution.blaze.plan.NativeSortExec
import org.apache.spark.sql.execution.blaze.plan.NativeSortMergeJoinExec
import org.apache.spark.sql.execution.blaze.plan.NativeBroadcastHashJoinExec
import org.apache.spark.sql.execution.blaze.plan.NativeUnionExec
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.blaze.plan.NativeFilterExec
import org.apache.spark.sql.execution.blaze.plan.NativeRenameColumnsExec
import org.apache.spark.SparkEnv

import org.apache.spark.sql.blaze.BlazeConvertStrategy.isNeverConvert
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.aggregate.Final
import org.apache.spark.sql.catalyst.expressions.aggregate.Partial
import org.apache.spark.sql.catalyst.expressions.aggregate.PartialMerge
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.blaze.plan.NativeLocalLimitExec
import org.apache.spark.sql.execution.blaze.plan.NativeGlobalLimitExec
import org.apache.spark.sql.execution.blaze.plan.NativeTakeOrderedExec
import org.apache.spark.sql.execution.blaze.plan.Util
import org.apache.spark.sql.execution.ExpandExec
import org.apache.spark.sql.execution.blaze.plan.ArrowBroadcastExchangeBase
import org.apache.spark.sql.execution.blaze.plan.ArrowShuffleExchangeBase
import org.apache.spark.sql.execution.blaze.plan.NativeExpandExec
import org.apache.spark.sql.execution.exchange.BroadcastExchangeLike

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
    val converted = danglingConverted.head

    // insert necessary ConvertToUnsafeRowExec
    converted
      .transformUp {
        case exec @ (
              _: SortExec | _: CollectLimitExec | _: BroadcastExchangeExec |
              _: SortMergeJoinExec | _: WindowExec | _: ObjectHashAggregateExec |
              _: DataWritingCommandExec | _: TakeOrderedAndProjectExec | _: ShuffleExchangeExec |
              _: CartesianProductExec
            ) =>
          exec.mapChildren(child => convertToUnsafeRow(child))
        case exec => exec
      }
  }

  def convertSparkPlan(exec: SparkPlan): SparkPlan = {
    val sparkPlanShims = Shims.get.sparkPlanShims
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
      case e: LocalLimitExec if enableLocalLimit => // local limit
        tryConvert(e, convertLocalLimitExec)
      case e: GlobalLimitExec if enableGlobalLimit => // global limit
        tryConvert(e, convertGlobalLimitExec)
      case e: TakeOrderedAndProjectExec if enableTakeOrderedAndProject =>
        tryConvert(e, convertTakeOrderedAndProjectExec)
      case e: HashAggregateExec if enableAggr => // aggregate
        tryConvert(e, convertHashAggregateExec)
      case e: ExpandExec if enableExpand => // aggregate
        tryConvert(e, convertExpandExec)

      case e if sparkPlanShims.isShuffleQueryStageInput(e) && sparkPlanShims.isNative(e) =>
        ForceNativeExecutionWrapper(e)

      case exec =>
        exec.setTagValue(convertibleTag, false)
        exec.setTagValue(convertStrategyTag, NeverConvert)
        exec
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
    logDebug(
      s"Converting ShuffleExchangeExec: ${Shims.get.sparkPlanShims.simpleStringWithNodeId(exec)}")

    assert(
      exec.outputPartitioning.numPartitions == 1 || exec.outputPartitioning
        .isInstanceOf[HashPartitioning])

    val convertedChild = outputPartitioning match {
      case p if p.isInstanceOf[HashPartitioning] || p.numPartitions == 1 =>
        convertToNative(child)
      case _ => child
    }
    Shims.get.shuffleShims
      .createArrowShuffleExchange(outputPartitioning, addRenameColumnsExec(convertedChild))
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
    logDebug(
      s"Converting FileSourceScanExec: ${Shims.get.sparkPlanShims.simpleStringWithNodeId(exec)}")
    logDebug(s"  relation: ${relation}")
    logDebug(s"  relation.location: ${relation.location}")
    logDebug(s"  output: ${output}")
    logDebug(s"  requiredSchema: ${requiredSchema}")
    logDebug(s"  partitionFilters: ${partitionFilters}")
    logDebug(s"  optionalBucketSet: ${optionalBucketSet}")
    logDebug(s"  dataFilters: ${dataFilters}")
    logDebug(s"  tableIdentifier: ${tableIdentifier}")
    return Shims.get.parquetScanShims.createParquetScan(exec)
  }

  def convertProjectExec(exec: ProjectExec): SparkPlan = {
    val (projectList, child) = (exec.projectList, exec.child)
    logDebug(s"Converting ProjectExec: ${Shims.get.sparkPlanShims.simpleStringWithNodeId(exec)}")
    projectList.foreach(p => logDebug(s"  projectExpr: ${p}"))
    NativeProjectExec(projectList, addRenameColumnsExec(convertToNative(child)))
  }

  def convertFilterExec(exec: FilterExec): SparkPlan =
    exec match {
      case exec: FilterExec =>
        logDebug(
          s"Converting FilterExec: ${Shims.get.sparkPlanShims.simpleStringWithNodeId(exec)}")
        logDebug(s"  condition: ${exec.condition}")
        NativeFilterExec(exec.condition, addRenameColumnsExec(convertToNative(exec.child)))
      case _ =>
        logDebug(s"Ignoring FilterExec: ${Shims.get.sparkPlanShims.simpleStringWithNodeId(exec)}")
        exec
    }

  def convertSortExec(exec: SortExec): SparkPlan = {
    val (sortOrder, global, child) = (exec.sortOrder, exec.global, exec.child)
    logDebug(s"Converting SortExec: ${Shims.get.sparkPlanShims.simpleStringWithNodeId(exec)}")
    logDebug(s"  global: ${global}")
    sortOrder.foreach(s => logDebug(s"  sortOrder: ${s}"))
    NativeSortExec(sortOrder, global, addRenameColumnsExec(convertToNative(child)))
  }

  def convertUnionExec(exec: UnionExec): SparkPlan = {
    logDebug(s"Converting UnionExec: ${Shims.get.sparkPlanShims.simpleStringWithNodeId(exec)}")
    NativeUnionExec(exec.children.map(child => {
      addRenameColumnsExec(convertToNative(child))
    }))
  }

  def convertSortMergeJoinExec(exec: SortMergeJoinExec): SparkPlan = {
    val (leftKeys, rightKeys, joinType, condition, left, right) =
      (exec.leftKeys, exec.rightKeys, exec.joinType, exec.condition, exec.left, exec.right)
    logDebug(
      s"Converting SortMergeJoinExec: ${Shims.get.sparkPlanShims.simpleStringWithNodeId(exec)}")
    assert(condition.isEmpty, "SortMergeJoin with post filter not yet supported")
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
      None,
      addRenameColumnsExec(nativeLeft),
      addRenameColumnsExec(nativeRight))
    val smj = NativeSortMergeJoinExec(
      smjOrig.left,
      smjOrig.right,
      smjOrig.leftKeys,
      smjOrig.rightKeys,
      smjOrig.output,
      smjOrig.outputPartitioning,
      smjOrig.outputOrdering,
      smjOrig.joinType)

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
      logDebug(
        s"Converting BroadcastHashJoinExec: ${Shims.get.sparkPlanShims.simpleStringWithNodeId(exec)}")
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
          joinType match { // reverse join type
            case Inner => Inner
            case FullOuter => FullOuter
            case LeftOuter => RightOuter
            case RightOuter => LeftOuter
            case _ =>
              throw new NotImplementedError(
                "BHJ Semi/Anti join with BuildRight is not yet supported")
          }
      }

      val bhj = NativeBroadcastHashJoinExec(
        addRenameColumnsExec(hashed),
        addRenameColumnsExec(nativeProbed),
        modifiedHashedKeys,
        modifiedProbedKeys,
        condition,
        modifiedJoinType)

      if (needPostProject) {
        buildPostJoinProject(bhj, exec.output)
      } else {
        bhj
      }
    } catch {
      case e @ (_: NotImplementedError | _: Exception) =>
        val underlyingBroadcast = exec.buildSide match {
          case BuildLeft => getUnderlyingBroadcast(exec.left)
          case BuildRight => getUnderlyingBroadcast(exec.right)
        }
        underlyingBroadcast.setTagValue(ArrowBroadcastExchangeBase.nativeExecutionTag, false)
        exec
    }
  }

  def convertBroadcastExchangeExec(exec: SparkPlan): SparkPlan = {
    exec match {
      case exec: BroadcastExchangeExec =>
        logDebug(
          s"Converting BroadcastExchangeExec: ${Shims.get.sparkPlanShims.simpleStringWithNodeId(exec)}")
        val converted =
          Shims.get.broadcastShims.createArrowBroadcastExchange(exec.mode, exec.child)
        converted.setTagValue(ArrowBroadcastExchangeBase.nativeExecutionTag, true)
        return converted
    }
    exec
  }

  def convertLocalLimitExec(exec: LocalLimitExec): SparkPlan = {
    logDebug(
      s"Converting LocalLimitExec: ${Shims.get.sparkPlanShims.simpleStringWithNodeId(exec)}")
    NativeLocalLimitExec(exec.limit.toLong, exec.child)
  }

  def convertGlobalLimitExec(exec: GlobalLimitExec): SparkPlan = {
    logDebug(
      s"Converting GlobalLimitExec: ${Shims.get.sparkPlanShims.simpleStringWithNodeId(exec)}")
    NativeGlobalLimitExec(exec.limit.toLong, exec.child)
  }

  def convertTakeOrderedAndProjectExec(exec: TakeOrderedAndProjectExec): SparkPlan = {
    logDebug(
      s"Converting TakeOrderedAndProjectExec: ${Shims.get.sparkPlanShims.simpleStringWithNodeId(exec)}")
    val nativeTakeOrdered =
      NativeTakeOrderedExec(exec.limit, exec.sortOrder, convertToNative(exec.child))

    if (exec.projectList != exec.child.output) {
      val project = ProjectExec(exec.projectList, nativeTakeOrdered)
      tryConvert(project, convertProjectExec)
    } else {
      nativeTakeOrdered
    }
  }

  def convertHashAggregateExec(exec: HashAggregateExec): SparkPlan = {
    logDebug(
      s"Converting HashAggregateExec: ${Shims.get.sparkPlanShims.simpleStringWithNodeId(exec)}")
    val nativeAggr = NativeHashAggregateExec(
      exec.requiredChildDistributionExpressions,
      exec.groupingExpressions,
      exec.aggregateExpressions,
      exec.aggregateAttributes,
      addRenameColumnsExec(convertToNative(exec.child)))

    nativeAggr.aggrMode match {
      case Partial | PartialMerge =>
        nativeAggr
      case Final =>
        try {
          NativeProjectExec(exec.resultExpressions, nativeAggr)
        } catch {
          case e @ (_: NotImplementedError | _: AssertionError | _: Exception) =>
            logWarning(
              s"Error projecting resultExpressions, failback to non-native projection: " +
                s"${e.getMessage}")
            ConvertToNativeExec(ProjectExec(exec.resultExpressions, nativeAggr))
        }
    }
  }

  def convertExpandExec(exec: ExpandExec): SparkPlan = {
    logDebug(s"Converting ExpandExec: ${Shims.get.sparkPlanShims.simpleStringWithNodeId(exec)}")
    logDebug(s"  projections: ${exec.projections}")
    NativeExpandExec(
      exec.projections,
      exec.output,
      addRenameColumnsExec(convertToNative(exec.child)))
  }

  def convertToUnsafeRow(exec: SparkPlan): SparkPlan = {
    if (!NativeHelper.isNative(exec)) {
      return exec
    }
    ConvertToUnsafeRowExec(exec)
  }

  def convertToNative(exec: SparkPlan): SparkPlan = {
    exec match {
      case exec if NativeHelper.isNative(exec) => exec
      case exec =>
        assert(exec.find(_.isInstanceOf[DataWritingCommandExec]).isEmpty)
        ConvertToNativeExec(exec)
    }
  }

  private def addRenameColumnsExec(exec: SparkPlan): SparkPlan = {
    def needRenameColumns(exec: SparkPlan): Boolean = {
      Shims.get.sparkPlanShims.needRenameColumns(exec)
    }
    if (needRenameColumns(exec)) {
      return NativeRenameColumnsExec(exec, exec.output.map(Util.getFieldNameByExprId))
    }
    exec
  }

  private def buildJoinColumnsProject(
      child: SparkPlan,
      joinKeys: Seq[Expression]): (Seq[AttributeReference], NativeProjectExec) = {
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
    (transformedKeys, NativeProjectExec(child.output ++ extraProjectList, child))
  }

  private def buildPostJoinProject(
      child: SparkPlan,
      output: Seq[Attribute]): NativeProjectExec = {
    val projectList = output
      .filter(!_.name.startsWith("JOIN_KEY:"))
      .map(
        attr =>
          AttributeReference(attr.name, attr.dataType, attr.nullable, attr.metadata)(
            attr.exprId,
            attr.qualifier))
    NativeProjectExec(projectList, child)
  }

  @tailrec
  private def getUnderlyingBroadcast(exec: SparkPlan): BroadcastExchangeLike = {
    exec match {
      case exec: BroadcastExchangeLike => exec
      case exec: UnaryExecNode =>
        getUnderlyingBroadcast(exec.child)
    }
  }

}
