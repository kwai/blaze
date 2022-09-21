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
import org.apache.spark.sql.catalyst.expressions.aggregate.Partial
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.catalyst.plans.FullOuter
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.catalyst.plans.RightOuter
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec}
import org.apache.spark.sql.execution.blaze.plan.ArrowShuffleExchangeExec
import org.apache.spark.sql.execution.blaze.plan.NativeParquetScanExec
import org.apache.spark.sql.execution.blaze.plan.NativeProjectExec
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.execution.{
  CollectLimitExec,
  FileSourceScanExec,
  FilterExec,
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
import org.apache.spark.sql.execution.blaze.plan.ArrowBroadcastExchangeExec
import org.apache.spark.sql.execution.blaze.plan.ConvertToUnsafeRowExec
import org.apache.spark.sql.execution.blaze.plan.NativeSortExec
import org.apache.spark.sql.execution.blaze.plan.NativeSortMergeJoinExec
import org.apache.spark.sql.execution.blaze.plan.NativeBroadcastHashJoinExec
import org.apache.spark.sql.execution.blaze.plan.NativeUnionExec
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.joins.BuildLeft
import org.apache.spark.sql.execution.joins.BuildRight
import org.apache.spark.sql.execution.blaze.plan.NativeFilterExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStage
import org.apache.spark.sql.execution.blaze.plan.NativeRenameColumnsExec
import org.apache.spark.SparkEnv
import org.apache.spark.sql.blaze.BlazeConvertStrategy.idTag
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.execution.adaptive.QueryStage
import org.apache.spark.sql.execution.adaptive.QueryStageInput
import org.apache.spark.sql.execution.command.DataWritingCommandExec
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
  val enableAggr: Boolean =
    SparkEnv.get.conf.getBoolean("spark.blaze.enable.aggr", defaultValue = false)

  def convertSparkPlanRecursively(exec: SparkPlan): SparkPlan = {
    exec
      .transformUp {
        case exec if BlazeConvertStrategy.isNeverConvert(exec) => exec
        case exec => convertSparkPlan(exec)
      }
      .transformUp {
        case exec @ (
              _: SortExec | _: CollectLimitExec | _: BroadcastExchangeExec |
              _: SortMergeJoinExec | _: WindowExec | _: ObjectHashAggregateExec |
              _: DataWritingCommandExec | _: TakeOrderedAndProjectExec | _: ShuffleExchangeExec
            ) =>
          exec.mapChildren(child => convertToUnsafeRow(child))
        case exec => exec
      }
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
      case e: HashAggregateExec if enableAggr => // aggregate
        tryConvert(e, convertHashAggregateExec)

      case exec =>
        exec.setTagValue(convertibleTag, false)
        exec.setTagValue(convertStrategyTag, NeverConvert)
        exec
    }
  }

  def tryConvert[T <: SparkPlan](exec: T, convert: T => SparkPlan): SparkPlan = {
    try {
      def setLogicalLink(exec: SparkPlan, basedExec: SparkPlan): SparkPlan = {
        if (basedExec.logicalLink.isDefined && exec.logicalLink.isEmpty) {
          exec.setLogicalLink(basedExec.logicalLink.get)
          exec.children.foreach(setLogicalLink(_, basedExec))
        }
        exec.setTagValue(idTag, exec.getTagValue(idTag).orNull)
        exec.setTagValue(convertibleTag, true)
        exec
      }
      setLogicalLink(convert(exec), exec)

    } catch {
      case e @ (_: NotImplementedError | _: AssertionError | _: Exception) =>
        logWarning(s"Error converting exec: ${exec.getClass.getSimpleName}: ${e.getMessage}")
        exec.setTagValue(convertibleTag, false)
        exec.setTagValue(convertStrategyTag, NeverConvert)
        exec
    }
  }

  def convertShuffleExchangeExec(exec: ShuffleExchangeExec): SparkPlan = {
    val ShuffleExchangeExec(outputPartitioning, child) = exec
    logDebug(s"Converting ShuffleExchangeExec: ${exec.simpleStringWithNodeId}")

    val convertedChild = outputPartitioning match {
      case _: HashPartitioning if BlazeConvertStrategy.preferNativeShuffle =>
        convertToNative(child)
      case _ => child
    }
    ArrowShuffleExchangeExec(outputPartitioning, addRenameColumnsExec(convertedChild))
  }

  def convertFileSourceScanExec(exec: FileSourceScanExec): SparkPlan = {
    val FileSourceScanExec(
      relation,
      output,
      requiredSchema,
      partitionFilters,
      optionalBucketSet,
      dataFilters,
      tableIdentifier,
      isHive) = exec
    logDebug(s"Converting FileSourceScanExec: ${exec.simpleStringWithNodeId}")
    logDebug(s"  relation: ${relation}")
    logDebug(s"  relation.location: ${relation.location}")
    logDebug(s"  output: ${output}")
    logDebug(s"  requiredSchema: ${requiredSchema}")
    logDebug(s"  partitionFilters: ${partitionFilters}")
    logDebug(s"  optionalBucketSet: ${optionalBucketSet}")
    logDebug(s"  dataFilters: ${dataFilters}")
    logDebug(s"  tableIdentifier: ${tableIdentifier}")
    if (relation.fileFormat.isInstanceOf[ParquetFileFormat]) {
      return NativeParquetScanExec(exec)
    }
    exec
  }

  def convertProjectExec(exec: ProjectExec): SparkPlan =
    exec match {
      case exec: ProjectExec =>
        logDebug(s"Converting ProjectExec: ${exec.simpleStringWithNodeId()}")
        exec.projectList.foreach(p => logDebug(s"  projectExpr: ${p}"))
        NativeProjectExec(exec.projectList, addRenameColumnsExec(convertToNative(exec.child)))
      case _ =>
        logDebug(s"Ignoring ProjectExec: ${exec.simpleStringWithNodeId()}")
        exec
    }

  def convertFilterExec(exec: FilterExec): SparkPlan =
    exec match {
      case exec: FilterExec =>
        logDebug(s"  condition: ${exec.condition}")
        NativeFilterExec(exec.condition, addRenameColumnsExec(convertToNative(exec.child)))
      case _ =>
        logDebug(s"Ignoring FilterExec: ${exec.simpleStringWithNodeId()}")
        exec
    }

  def convertSortExec(exec: SortExec): SparkPlan = {
    exec match {
      case SortExec(sortOrder, global, child, _) =>
        logDebug(s"Converting SortExec: ${exec.simpleStringWithNodeId()}")
        logDebug(s"  global: ${global}")
        exec.sortOrder.foreach(s => logDebug(s"  sortOrder: ${s}"))
        NativeSortExec(sortOrder, global, addRenameColumnsExec(convertToNative(child)))
      case _ =>
        logDebug(s"Ignoring SortExec: ${exec.simpleStringWithNodeId()}")
        exec
    }
  }

  def convertUnionExec(exec: UnionExec): SparkPlan = {
    exec match {
      case UnionExec(children) =>
        logDebug(s"Converting UnionExec: ${exec.simpleStringWithNodeId()}")
        for (a <- children) {
          assert(a.output.map(_.dataType) == exec.output.map(_.dataType))
          assert(a.output.map(_.nullable) == exec.output.map(_.nullable))
        }
        NativeUnionExec(children.map(child => {
          addRenameColumnsExec(convertToNative(child))
        }))
      case _ =>
        logDebug(s"Ignoring UnionExec: ${exec.simpleStringWithNodeId()}")
        exec
    }
  }

  def convertSortMergeJoinExec(exec: SortMergeJoinExec): SparkPlan = {
    // if (exec.isSkewJoin) {
    //   throw new NotImplementedError("skew join is not yet supported")
    // }
    exec match {
      case SortMergeJoinExec(leftKeys, rightKeys, joinType, condition, left, right) =>
        logDebug(s"Converting SortMergeJoinExec: ${exec.simpleStringWithNodeId()}")

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

        val smj = NativeSortMergeJoinExec(
          addRenameColumnsExec(nativeLeft),
          addRenameColumnsExec(nativeRight),
          modifiedLeftKeys,
          modifiedRightKeys,
          exec.output,
          exec.outputPartitioning,
          exec.outputOrdering,
          joinType)

        val postProjectedSmj = if (needPostProject) {
          buildPostJoinProject(smj, exec.output)
        } else {
          smj
        }

        val postProjectExprIds = postProjectedSmj.output.map(_.exprId.id).toSet
        val conditionedSmj = condition match {
          case Some(condition) =>
            if (!condition.references.map(_.exprId.id).toSet.subsetOf(postProjectExprIds)) {
              throw new NotImplementedError(
                "SMJ post filter with columns not existed in join output is not yet supported")
            }
            NativeFilterExec(condition, postProjectedSmj)
          case None => postProjectedSmj
        }
        return conditionedSmj
    }
    logDebug(s"Ignoring SortMergeJoinExec: ${exec.simpleStringWithNodeId()}")
    exec
  }

  def convertBroadcastHashJoinExec(exec: BroadcastHashJoinExec): SparkPlan = {
    try {
      exec match {
        case BroadcastHashJoinExec(
              leftKeys,
              rightKeys,
              joinType,
              buildSide,
              condition,
              left,
              right) =>
          var (hashed, hashedKeys, nativeProbed, probedKeys) = buildSide match {
            case BuildRight =>
              assert(NativeSupports.isNative(right))
              val convertedLeft = convertToNative(left)
              (right, rightKeys, convertedLeft, leftKeys)

            case BuildLeft =>
              assert(NativeSupports.isNative(left))
              val convertedRight = convertToNative(right)
              (left, leftKeys, convertedRight, rightKeys)

            case _ =>
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
            modifiedJoinType)

          val postProjectedBhj = if (needPostProject) {
            buildPostJoinProject(bhj, exec.output)
          } else {
            bhj
          }

          val postProjectExprIds = postProjectedBhj.output.map(_.exprId.id).toSet
          val conditionedBhj = condition match {
            case Some(condition) =>
              if (!condition.references.map(_.exprId.id).toSet.subsetOf(postProjectExprIds)) {
                throw new NotImplementedError(
                  "BHJ post filter with columns not existed in join output is not yet supported")
              }
              NativeFilterExec(condition, postProjectedBhj)
            case None => postProjectedBhj
          }
          return conditionedBhj
      }
    } catch {
      case e @ (_: NotImplementedError | _: Exception) =>
        val underlyingBroadcast = exec.buildSide match {
          case BuildLeft => getUnderlyingBroadcast(exec.left)
          case BuildRight => getUnderlyingBroadcast(exec.right)
        }
        underlyingBroadcast.setTagValue(ArrowBroadcastExchangeExec.nativeExecutionTag, false)
    }
    logDebug(s"Ignoring BroadcastHashJoinExec: ${exec.simpleStringWithNodeId()}")
    exec
  }

  def convertBroadcastExchangeExec(exec: SparkPlan): SparkPlan = {
    exec match {
      case exec: BroadcastExchangeExec =>
        val converted = ArrowBroadcastExchangeExec(exec.mode, exec.child)
        converted.setTagValue(ArrowBroadcastExchangeExec.nativeExecutionTag, true)
        return converted
    }
    exec
  }

  def convertHashAggregateExec(exec: SparkPlan): SparkPlan = {
    exec match {
      case exec: HashAggregateExec =>
        val converted = NativeHashAggregateExec(
          exec.requiredChildDistributionExpressions,
          exec.groupingExpressions,
          exec.aggregateExpressions,
          exec.aggregateAttributes,
          exec.resultExpressions,
          convertToNative(exec.child))

        if (converted.aggrMode == Partial) {
          return converted
        }
        NativeProjectExec(exec.resultExpressions, converted, addTypeCast = true)

      case exec =>
        return exec
    }
    exec
  }

  def convertToUnsafeRow(exec: SparkPlan): SparkPlan = {
    if (!NativeSupports.isNative(exec)) {
      return exec
    }
    ConvertToUnsafeRowExec(exec)
  }

  def convertToNative(exec: SparkPlan): SparkPlan = {
    exec match {
      case exec if NativeSupports.isNative(exec) => exec
      case exec =>
        assert(!exec.isInstanceOf[DataWritingCommandExec]) // not convertible to native
        ConvertToNativeExec(exec)
    }
  }

  private def addRenameColumnsExec(exec: SparkPlan): SparkPlan = {
    if (needRenameColumns(exec)) {
      return NativeRenameColumnsExec(exec, exec.output.map(a => s"#${a.exprId.id}"))
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

  private def needRenameColumns(exec: SparkPlan): Boolean = {
    if (exec.output.isEmpty) {
      return false
    }
    exec match {
      case exec: QueryStageInput =>
        needRenameColumns(exec.childStage) || exec.output != exec.childStage.output
      case exec: QueryStage =>
        needRenameColumns(exec.child)
      case _: NativeParquetScanExec | _: NativeUnionExec | _: ReusedExchangeExec => true
      case _ => false
    }
  }

  @tailrec
  private def getUnderlyingBroadcast(exec: SparkPlan): SparkPlan = {
    exec match {
      case _: BroadcastExchangeExec | _: ArrowBroadcastExchangeExec => exec
      case exec: BroadcastQueryStage => exec.child
      case exec: UnaryExecNode =>
        getUnderlyingBroadcast(exec.child)
    }
  }

  implicit class ImplicitSimpleStringWithNodeId(sparkPlan: SparkPlan) {
    def simpleStringWithNodeId(): String = {
      sparkPlan.simpleString
    }
  }

  implicit class ImplicitLogicalLink(sparkPlan: SparkPlan) {
    def logicalLink: Option[LogicalPlan] = None
    def setLogicalLink(logicalPlan: LogicalPlan): Unit = {}
  }
}
