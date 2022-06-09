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

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.blaze.execution.ArrowShuffleExchangeExec301
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.CollectLimitExec
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.FilterExec
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.execution.SortExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnionExec
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.blaze.execution.ArrowBroadcastExchangeExec
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.catalyst.plans.FullOuter
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.catalyst.plans.RightOuter
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.ColumnarRule
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec
import org.apache.spark.sql.execution.adaptive.CustomShuffleReaderExec
import org.apache.spark.sql.execution.adaptive.ShuffleQueryStageExec
import org.apache.spark.sql.execution.exchange.BroadcastExchangeLike
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.execution.joins.BuildLeft
import org.apache.spark.sql.execution.joins.BuildRight
import org.apache.spark.sql.execution.plan.NativeBroadcastHashJoinExec
import org.apache.spark.sql.execution.plan.NativeFilterExec
import org.apache.spark.sql.execution.plan.NativeParquetScanExec
import org.apache.spark.sql.execution.plan.NativeProjectExec
import org.apache.spark.sql.execution.plan.NativeRenameColumnsExec
import org.apache.spark.sql.execution.plan.NativeSortExec
import org.apache.spark.sql.execution.plan.NativeSortMergeJoinExec
import org.apache.spark.sql.execution.plan.NativeUnionExec
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.aggregate.HashAggregateExec

class BlazeSparkSessionExtension extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    SparkEnv.get.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
    SparkEnv.get.conf.set(SQLConf.ADAPTIVE_EXECUTION_FORCE_APPLY.key, "true")
    logInfo("org.apache.spark.BlazeSparkSessionExtension enabled")

    extensions.injectColumnar(sparkSession => {
      BlazeColumnarOverrides(sparkSession)
    })
  }
}

case class BlazeColumnarOverrides(sparkSession: SparkSession) extends ColumnarRule with Logging {
  import org.apache.spark.sql.blaze.Util._

  override def preColumnarTransitions: Rule[SparkPlan] =
    sparkPlan => {
      // mark all skewed SMJ sorters
      sparkPlan.foreachUp {
        case SortMergeJoinExec(
              _,
              _,
              _,
              _,
              SortExec(_, _, sortChild1, _),
              SortExec(_, _, sortChild2, _),
              true) =>
          sortChild1.setTagValue(skewJoinSortChildrenTag, true)
          sortChild2.setTagValue(skewJoinSortChildrenTag, true)
        case _ =>
      }

      // transform supported plans to native
      var sparkPlanTransformed = sparkPlan
        .transformUp {
          case exec: ShuffleExchangeExec if enableNativeShuffle =>
            tryConvert(exec, convertShuffleExchangeExec)
          case exec: BroadcastExchangeExec if enableBhj =>
            tryConvert(exec, convertBroadcastExchangeExec)
          case exec: FileSourceScanExec if enableScan =>
            tryConvert(exec, convertFileSourceScanExec)
          case exec: ProjectExec if Util.enableProject =>
            tryConvert(exec, convertProjectExec)
          case exec: FilterExec if Util.enableFilter =>
            tryConvert(exec, convertFilterExec)
          case exec: SortExec if Util.enableSort =>
            tryConvert(exec, convertSortExec)
          case exec: UnionExec if Util.enableUnion =>
            tryConvert(exec, convertUnionExec)
          case exec: SortMergeJoinExec if enableSmj =>
            tryConvert(exec, convertSortMergeJoinExec)
          case exec: BroadcastHashJoinExec if enableBhj =>
            tryConvert(exec, convertBroadcastHashJoinExec)
          case exec => exec
        }
        .transformUp {
          // add ConvertToUnsafeRow before specified plans those require consuming unsafe rows
          case exec @ (
                _: SortExec | _: CollectLimitExec | _: BroadcastExchangeExec |
                _: SortMergeJoinExec | _: WindowExec
              ) =>
            exec.mapChildren(child => convertToUnsafeRow(child))
        }

      // wrap with ConvertUnsafeRowExec if top exec is native
      // val topNeededConvertToUnsafeRow =
      if (NativeSupports.isNative(sparkPlanTransformed)) {
        val topNative = NativeSupports.getUnderlyingNativePlan(sparkPlanTransformed)
        val topNeededConvertToUnsafeRow = topNative match {
          case _: ShuffleExchangeLike => false
          case _: BroadcastExchangeLike => false
          case _ => true
        }
        if (topNeededConvertToUnsafeRow) {
          sparkPlanTransformed = convertToUnsafeRow(sparkPlanTransformed)
        }
      }

      logDebug(s"Transformed spark plan after preColumnarTransitions:\n${sparkPlanTransformed
        .treeString(verbose = true, addSuffix = true, printOperatorId = true)}")
      sparkPlanTransformed
    }
}

private object Util extends Logging {
  private val ENABLE_OPERATION = "spark.blaze.enable."

  val enableNativeShuffle: Boolean =
    SparkEnv.get.conf.getBoolean(ENABLE_OPERATION + "shuffle", defaultValue = true)
  val enableScan: Boolean =
    SparkEnv.get.conf.getBoolean(ENABLE_OPERATION + "scan", defaultValue = true)
  val enableProject: Boolean =
    SparkEnv.get.conf.getBoolean(ENABLE_OPERATION + "project", defaultValue = true)
  val enableFilter: Boolean =
    SparkEnv.get.conf.getBoolean(ENABLE_OPERATION + "filter", defaultValue = true)
  val enableSort: Boolean =
    SparkEnv.get.conf.getBoolean(ENABLE_OPERATION + "sort", defaultValue = true)
  val enableUnion: Boolean =
    SparkEnv.get.conf.getBoolean(ENABLE_OPERATION + "union", defaultValue = true)
  val enableSmj: Boolean =
    SparkEnv.get.conf.getBoolean(ENABLE_OPERATION + "sortmergejoin", defaultValue = true)
  val enableBhj: Boolean =
    SparkEnv.get.conf.getBoolean(ENABLE_OPERATION + "broadcasthashjoin", defaultValue = true)
  val preferNativeShuffle: Boolean =
    SparkEnv.get.conf.getBoolean("spark.blaze.preferNativeShuffle", defaultValue = true)
  val preferNativeBhj: Boolean =
    SparkEnv.get.conf.getBoolean("spark.blaze.preferNativeBroadcastHashJoin", defaultValue = true)

  val skewJoinSortChildrenTag: TreeNodeTag[Boolean] = TreeNodeTag("skewJoinSortChildren")

  def tryConvert[T <: SparkPlan](exec: T, convert: T => SparkPlan): SparkPlan =
    try {
      def setLogicalLink(exec: SparkPlan, basedExec: SparkPlan): SparkPlan = {
        if (basedExec.logicalLink.isDefined && exec.logicalLink.isEmpty) {
          exec.setLogicalLink(basedExec.logicalLink.get)
          exec.children.foreach(setLogicalLink(_, basedExec))
        }
        exec
      }
      setLogicalLink(convert(exec), exec)
    } catch {
      case e @ (_: NotImplementedError | _: Exception) =>
        logWarning(s"Error converting exec: ${exec.getClass.getSimpleName}: ${e.getMessage}")
        exec
    }

  def convertShuffleExchangeExec(exec: ShuffleExchangeExec): SparkPlan = {
    val ShuffleExchangeExec(outputPartitioning, child, noUserSpecifiedNumPartition) = exec
    logDebug(s"Converting ShuffleExchangeExec: ${exec.simpleStringWithNodeId}")

    // NOTE: prefer native shuffling because we found it faster than jvm at the moment.
    val convertedChild = outputPartitioning match {
      case _: HashPartitioning if preferNativeShuffle && !NativeSupports.isNative(child) =>
        ConvertToNativeExec(child)
      case _ =>
        child
    }
    ArrowShuffleExchangeExec301(
      outputPartitioning,
      addRenameColumnsExec(convertedChild),
      noUserSpecifiedNumPartition)
  }

  def convertFileSourceScanExec(exec: FileSourceScanExec): SparkPlan = {
    val FileSourceScanExec(
      relation,
      output,
      requiredSchema,
      partitionFilters,
      optionalBucketSet,
      dataFilters,
      tableIdentifier) = exec
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
      case ProjectExec(projectList, child) if NativeSupports.isNative(child) =>
        logDebug(s"Converting ProjectExec: ${exec.simpleStringWithNodeId()}")
        exec.projectList.foreach(p => logDebug(s"  projectExpr: ${p}"))
        NativeProjectExec(projectList, addRenameColumnsExec(child))
      case _ =>
        logDebug(s"Ignoring ProjectExec: ${exec.simpleStringWithNodeId()}")
        exec
    }

  def convertFilterExec(exec: FilterExec): SparkPlan =
    exec match {
      case FilterExec(condition, child) if NativeSupports.isNative(child) =>
        logDebug(s"  condition: ${exec.condition}")
        NativeFilterExec(condition, addRenameColumnsExec(child))
      case _ =>
        logDebug(s"Ignoring FilterExec: ${exec.simpleStringWithNodeId()}")
        exec
    }

  def convertSortExec(exec: SortExec): SparkPlan = {
    if (exec.child.getTagValue(skewJoinSortChildrenTag).isDefined) {
      return exec // do not convert skewed join SMJ sorters
    }
    exec match {
      case SortExec(sortOrder, global, child, _) if NativeSupports.isNative(child) =>
        logDebug(s"Converting SortExec: ${exec.simpleStringWithNodeId()}")
        logDebug(s"  global: ${global}")
        exec.sortOrder.foreach(s => logDebug(s"  sortOrder: ${s}"))
        NativeSortExec(sortOrder, global, addRenameColumnsExec(child))
      case _ =>
        logDebug(s"Ignoring SortExec: ${exec.simpleStringWithNodeId()}")
        exec
    }
  }

  def convertUnionExec(exec: UnionExec): SparkPlan =
    exec match {
      case UnionExec(children) if children.forall(c => NativeSupports.isNative(c)) =>
        logDebug(s"Converting UnionExec: ${exec.simpleStringWithNodeId()}")
        NativeUnionExec(children.map(child => {
          addRenameColumnsExec(child)
        }))
      case _ =>
        logDebug(s"Ignoring UnionExec: ${exec.simpleStringWithNodeId()}")
        exec
    }

  def convertSortMergeJoinExec(exec: SortMergeJoinExec): SparkPlan = {
    if (exec.isSkewJoin) {
      throw new NotImplementedError("skew join is not yet supported")
    }
    exec match {
      case SortMergeJoinExec(leftKeys, rightKeys, joinType, condition, left, right, _) =>
        if (Seq(left, right).exists(NativeSupports.isNative)) {
          logDebug(s"Converting SortMergeJoinExec: ${exec.simpleStringWithNodeId()}")

          var nativeLeft = left match {
            case l if !NativeSupports.isNative(l) => ConvertToNativeExec(l)
            case l => l
          }
          var nativeRight = right match {
            case r if !NativeSupports.isNative(r) => ConvertToNativeExec(r)
            case r => r
          }
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
            buildPostJoinProject(smj)
          } else {
            smj
          }

          val conditionedSmj = condition match {
            case Some(condition) =>
              if (condition.references.exists(a => !smj.output.contains(a))) {
                throw new NotImplementedError(
                  "SMJ post filter with columns not existed in join output is not yet supported")
              }
              NativeFilterExec(condition, postProjectedSmj)
            case None => postProjectedSmj
          }
          return conditionedSmj
        }
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
            case BuildRight if preferNativeBhj || NativeSupports.isNative(left) =>
              val convertedLeft = left match {
                case left if NativeSupports.isNative(left) => left
                case left => ConvertToNativeExec(left)
              }
              (right, rightKeys, convertedLeft, leftKeys)

            case BuildLeft if preferNativeBhj || NativeSupports.isNative(right) =>
              val convertedRight = right match {
                case right if NativeSupports.isNative(right) => right
                case right => ConvertToNativeExec(right)
              }
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
            exec.outputPartitioning,
            exec.outputOrdering,
            modifiedJoinType)

          val postProjectedBhj = if (needPostProject) {
            buildPostJoinProject(bhj)
          } else {
            bhj
          }

          val conditionedBhj = condition match {
            case Some(condition) =>
              if (condition.references.exists(a => !bhj.output.contains(a))) {
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
        val converted = ArrowBroadcastExchangeExec(exec.mode, convertToUnsafeRow(exec.child))
        converted.setTagValue(ArrowBroadcastExchangeExec.nativeExecutionTag, true)
        return converted
    }
    exec
  }

  def convertToUnsafeRow(exec: SparkPlan): SparkPlan = {
    if (!NativeSupports.isNative(exec)) {
      return exec
    }
    ConvertToUnsafeRowExec(exec)
  }

  def addRenameColumnsExec(exec: SparkPlan): SparkPlan = {
    if (needRenameColumns(exec)) {
      return NativeRenameColumnsExec(exec, exec.output.map(_.toString()))
    }
    exec
  }

  def buildJoinColumnsProject(
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

  def buildPostJoinProject(child: SparkPlan): NativeProjectExec = {
    val projectList = child.output
      .filter(!_.name.startsWith("JOIN_KEY:"))
      .map(
        attr =>
          AttributeReference(attr.name, attr.dataType, attr.nullable, attr.metadata)(
            attr.exprId,
            attr.qualifier))
    NativeProjectExec(projectList, child)
  }

  @tailrec
  def needRenameColumns(exec: SparkPlan): Boolean = {
    exec match {
      case exec: ShuffleQueryStageExec => needRenameColumns(exec.plan)
      case exec: BroadcastQueryStageExec => needRenameColumns(exec.plan)
      case exec: CustomShuffleReaderExec => needRenameColumns(exec.child)
      case _: NativeParquetScanExec | _: NativeUnionExec | _: ReusedExchangeExec => true
      case _ => false
    }
  }

  @tailrec
  def getUnderlyingBroadcast(exec: SparkPlan): SparkPlan = {
    exec match {
      case exec: BroadcastExchangeLike =>
        exec
      case exec: BroadcastQueryStageExec =>
        exec.plan
      case exec: UnaryExecNode =>
        getUnderlyingBroadcast(exec.child)
    }
  }
}
