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

package org.apache.spark.sql.blaze

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.blaze.execution.ArrowShuffleExchangeExec301
import org.apache.spark.sql.blaze.plan.NativeFilterExec
import org.apache.spark.sql.blaze.plan.NativeParquetScanExec
import org.apache.spark.sql.blaze.plan.NativeProjectExec
import org.apache.spark.sql.blaze.plan.NativeSortExec
import org.apache.spark.sql.blaze.plan.NativeUnionExec
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
import org.apache.spark.sql.blaze.plan.NativeSortMergeJoinExec
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.NamedExpression

class BlazeSparkSessionExtension extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    SparkEnv.get.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
    SparkEnv.get.conf.set(SQLConf.ADAPTIVE_EXECUTION_FORCE_APPLY.key, "true")
    logInfo("org.apache.spark.BlazeSparkSessionExtension enabled")

    extensions.injectQueryStagePrepRule(sparkSession => {
      BlazeQueryStagePrepOverrides(sparkSession)
    })
  }
}

case class BlazeQueryStagePrepOverrides(sparkSession: SparkSession)
    extends Rule[SparkPlan]
    with Logging {
  val ENABLE_OPERATION = "spark.blaze.enable."
  val enableWholeStage =
    SparkEnv.get.conf.getBoolean(ENABLE_OPERATION + "wholestagewrapper", true)
  val enableNativeShuffle = SparkEnv.get.conf.getBoolean(ENABLE_OPERATION + "shuffle", true)
  val enableScan = SparkEnv.get.conf.getBoolean(ENABLE_OPERATION + "scan", true)
  val enableProject = SparkEnv.get.conf.getBoolean(ENABLE_OPERATION + "project", true)
  val enableFilter = SparkEnv.get.conf.getBoolean(ENABLE_OPERATION + "filter", true)
  val enableSort = SparkEnv.get.conf.getBoolean(ENABLE_OPERATION + "sort", true)
  val enableUnion = SparkEnv.get.conf.getBoolean(ENABLE_OPERATION + "union", true)
  val enableSmj = SparkEnv.get.conf.getBoolean(ENABLE_OPERATION + "sortmergejoin", true)

  override def apply(sparkPlan: SparkPlan): SparkPlan = {
    var sparkPlanTransformed = sparkPlan
      .transformUp { // transform supported plans to native
        case exec: ShuffleExchangeExec if enableNativeShuffle =>
          tryConvert(exec, convertShuffleExchangeExec)
        case exec: FileSourceScanExec if enableScan => tryConvert(exec, convertFileSourceScanExec)
        case exec: ProjectExec if enableProject => tryConvert(exec, convertProjectExec)
        case exec: FilterExec if enableFilter => tryConvert(exec, convertFilterExec)
        case exec: SortExec if enableSort => tryConvert(exec, convertSortExec)
        case exec: UnionExec if enableUnion => tryConvert(exec, convertUnionExec)
        case exec: SortMergeJoinExec if enableSmj => tryConvert(exec, convertSortMergeJoinExec)
        case exec =>
          log.info(s"Ignore unsupported exec: ${exec.simpleStringWithNodeId()}")
          exec
      }
      .transformUp { // add ConvertToUnsafeRow before specified plans those require consuming unsafe rows
        case exec @ (
              _: SortExec | _: CollectLimitExec | _: BroadcastExchangeExec |
              _: SortMergeJoinExec | _: WindowExec
            ) =>
          exec.mapChildren(child => convertToUnsafeRow(child))
      }

    // wrap with ConvertUnsafeRowExec if top exec is native
    if (NativeSupports.isNative(sparkPlanTransformed)) {
      sparkPlanTransformed = convertToUnsafeRow(sparkPlanTransformed)
    }

    logDebug(s"Transformed spark plan:\n${sparkPlanTransformed
      .treeString(verbose = true, addSuffix = true, printOperatorId = true)}")
    sparkPlanTransformed
  }

  private def tryConvert[T <: SparkPlan](exec: T, convert: T => SparkPlan): SparkPlan =
    try {
      val convertedExec = convert(exec)
      convertedExec
    } catch {
      case e @ (_: NotImplementedError | _: Exception) =>
        logWarning(s"Error converting exec: ${exec.getClass.getSimpleName}: ${e.getMessage}")
        exec
    }

  private def convertShuffleExchangeExec(exec: ShuffleExchangeExec): SparkPlan = {
    val ShuffleExchangeExec(outputPartitioning, child, noUserSpecifiedNumPartition) = exec
    logDebug(s"Converting ShuffleExchangeExec: ${exec.simpleStringWithNodeId}")
    ArrowShuffleExchangeExec301(outputPartitioning, child, noUserSpecifiedNumPartition)
  }

  private def convertFileSourceScanExec(exec: FileSourceScanExec): SparkPlan = {
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
      return NativeParquetScanExec(
        exec
      ) // note: supports exec.dataFilters for better performance?
    }
    exec
  }

  private def convertProjectExec(exec: ProjectExec): SparkPlan =
    exec match {
      case ProjectExec(projectList, child) if NativeSupports.isNative(child) =>
        logDebug(s"Converting ProjectExec: ${exec.simpleStringWithNodeId()}")
        exec.projectList.foreach(p => logDebug(s"  projectExpr: ${p}"))
        NativeProjectExec(projectList, child)
      case _ =>
        logDebug(s"Ignoring ProjectExec: ${exec.simpleStringWithNodeId()}")
        exec
    }

  private def convertFilterExec(exec: FilterExec): SparkPlan =
    exec match {
      case FilterExec(condition, child) if NativeSupports.isNative(child) =>
        logDebug(s"  condition: ${exec.condition}")
        NativeFilterExec(condition, child)
      case _ =>
        logDebug(s"Ignoring FilterExec: ${exec.simpleStringWithNodeId()}")
        exec
    }

  def convertSortExec(exec: SortExec): SparkPlan =
    exec match {
      case SortExec(sortOrder, global, child, _) if NativeSupports.isNative(child) =>
        logDebug(s"Converting SortExec: ${exec.simpleStringWithNodeId()}")
        logDebug(s"  global: ${global}")
        exec.sortOrder.foreach(s => logDebug(s"  sortOrder: ${s}"))
        NativeSortExec(sortOrder, global, child)
      case _ =>
        logDebug(s"Ignoring SortExec: ${exec.simpleStringWithNodeId()}")
        exec
    }

  def convertUnionExec(exec: UnionExec): SparkPlan =
    exec match {
      case UnionExec(children) if children.forall(c => NativeSupports.isNative(c)) =>
        logDebug(s"Converting UnionExec: ${exec.simpleStringWithNodeId()}")
        NativeUnionExec(children)
      case _ =>
        logDebug(s"Ignoring UnionExec: ${exec.simpleStringWithNodeId()}")
        exec
    }

  def convertSortMergeJoinExec(exec: SortMergeJoinExec): SparkPlan = {
    exec match {
      case SortMergeJoinExec(leftKeys, rightKeys, joinType, condition, left, right, isSkewJoin) =>
        if (Seq(left, right).exists(NativeSupports.isNative)) {
          logDebug(s"Converting SortMergeJoinExec: ${exec.simpleStringWithNodeId()}")

          val extraColumnPrefix = s"__dummy_smjkey_"
          var extraColumnId = 0

          def buildJoinColumnsProject(
              child: SparkPlan,
              joinKeys: Seq[Expression]): (Seq[AttributeReference], NativeProjectExec) = {
            val extraProjectList = ArrayBuffer[NamedExpression]()
            val transformedKeys = ArrayBuffer[AttributeReference]()

            joinKeys.foreach {
              case attr: AttributeReference => transformedKeys.append(attr)
              case expr =>
                val aliasExpr = Alias(expr, s"${extraColumnPrefix}_${extraColumnId}")()
                extraColumnId += 1
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

          def buildPostProject(child: NativeSortMergeJoinExec): NativeProjectExec = {
            val projectList = child.output
              .filter(!_.name.startsWith(extraColumnPrefix))
              .map(
                attr =>
                  AttributeReference(attr.name, attr.dataType, attr.nullable, attr.metadata)(
                    attr.exprId,
                    attr.qualifier))
            NativeProjectExec(projectList, child)
          }

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
            nativeLeft,
            nativeRight,
            modifiedLeftKeys,
            modifiedRightKeys,
            exec.output,
            exec.outputPartitioning,
            exec.outputOrdering,
            joinType)

          val postProjectedSmj = if (needPostProject) {
            buildPostProject(smj)
          } else {
            smj
          }

          val conditionedSmj = condition match {
            case Some(condition) => NativeFilterExec(condition, postProjectedSmj)
            case None => postProjectedSmj
          }
          return conditionedSmj
        }
    }
    logDebug(s"Ignoring SortMergeJoinExec: ${exec.simpleStringWithNodeId()}")
    exec
  }

  private def convertToUnsafeRow(exec: SparkPlan): SparkPlan = {
    exec match {
      case exec if NativeSupports.isNative(exec) => ConvertToUnsafeRowExec(exec)
      case exec => exec
    }
  }
}
