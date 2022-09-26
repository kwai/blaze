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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.adaptive.{QueryStage => QueryStageExec}
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.execution.CodegenSupport
import org.apache.spark.SparkEnv
import org.apache.spark.sql.execution.FilterExec
import org.apache.spark.sql.execution.SortExec
import org.apache.spark.sql.execution.UnionExec
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.catalyst.expressions.aggregate.Partial
import org.apache.spark.sql.execution.adaptive.ShuffleQueryStageInput
import org.apache.spark.sql.execution.adaptive.SkewedShuffleQueryStageInput
import org.apache.spark.sql.execution.GlobalLimitExec
import org.apache.spark.sql.execution.LocalLimitExec
import org.apache.spark.sql.types.TimestampType

object BlazeConvertStrategy extends Logging {
  import BlazeConverters._

  val continuousCodegenThreshold: Int =
    SparkEnv.get.conf.getInt("spark.blaze.continuous.codegen.threshold", 5)
  val preferNativeShuffle: Boolean =
    SparkEnv.get.conf.getBoolean("spark.blaze.prefer.native.shuffle", defaultValue = true)
  val preferNativeProject: Boolean =
    SparkEnv.get.conf.getBoolean("spark.blaze.prefer.native.project", defaultValue = false)
  val preferNativeFilter: Boolean =
    SparkEnv.get.conf.getBoolean("spark.blaze.prefer.native.filter", defaultValue = false)
  val preferNativeSort: Boolean =
    SparkEnv.get.conf.getBoolean("spark.blaze.prefer.native.sort", defaultValue = false)
  val preferNativeSmj: Boolean =
    SparkEnv.get.conf.getBoolean("spark.blaze.prefer.native.smj", defaultValue = false)
  val preferNativeBhj: Boolean =
    SparkEnv.get.conf.getBoolean("spark.blaze.prefer.native.bhj", defaultValue = false)
  val preferNativeLocalLimit: Boolean =
    SparkEnv.get.conf.getBoolean("spark.blaze.prefer.native.local.limit", defaultValue = false)
  val preferNativeGlobalLimit: Boolean =
    SparkEnv.get.conf.getBoolean("spark.blaze.prefer.native.global.limit", defaultValue = false)
  val preferNativeAggr: Boolean =
    SparkEnv.get.conf.getBoolean("spark.blaze.prefer.native.aggr", defaultValue = false)

  val neverConvertSkewJoinEnabled: Boolean =
    SparkEnv.get.conf
      .getBoolean("spark.blaze.strategy.enable.neverConvertSkewJoin", defaultValue = true)
  val neverConvertExecWithTimestampEnabled: Boolean =
    SparkEnv.get.conf
      .getBoolean(
        "spark.blaze.strategy.enable.neverConvertExecWithTimestamp",
        defaultValue = true)
  val neverConvertJoinsWithPostConditionEnabled: Boolean =
    SparkEnv.get.conf.getBoolean(
      "spark.blaze.strategy.enable.neverConvertJoinsWtihCondition",
      defaultValue = true)
  val alwaysConvertDirectSortMergeJoinEnabled: Boolean =
    SparkEnv.get.conf.getBoolean(
      "spark.blaze.strategy.enable.alwaysConvertDirectSortMergeJoin",
      defaultValue = true)
  val neverConvertContinuousCodegensEnabled: Boolean =
    SparkEnv.get.conf.getBoolean(
      "spark.blaze.strategy.enable.neverConvertContinuousCodegens",
      defaultValue = true)
  val neverConvertScanWithInconvertibleChildrenEnabled: Boolean =
    SparkEnv.get.conf.getBoolean(
      "spark.blaze.strategy.enable.neverConvertScanWithInconvertibleChildren",
      defaultValue = true)
  val neverConvertAggregatesChildrenEnabled: Boolean =
    SparkEnv.get.conf.getBoolean(
      "spark.blaze.strategy.enable.neverConvertAggregatesChildren",
      defaultValue = true)
  val neverConvertPartialAggregateShuffleExchangeEnabled: Boolean =
    SparkEnv.get.conf.getBoolean(
      "spark.blaze.strategy.enable.neverConvertAggregateShuffleExchange",
      defaultValue = true)

  val idTag: TreeNodeTag[UUID] = TreeNodeTag("blaze.id")
  val convertibleTag: TreeNodeTag[Boolean] = TreeNodeTag("blaze.convertible")
  val convertStrategyTag: TreeNodeTag[ConvertStrategy] = TreeNodeTag("blaze.convert.strategy")

  def apply(exec: SparkPlan): Unit = {
    exec.foreach(_.setTagValue(idTag, UUID.randomUUID()))
    exec.foreach(_.setTagValue(convertibleTag, true))
    exec.foreach(_.setTagValue(convertStrategyTag, Default))

    // try to convert all plans and fill convertible tag back to origin exec
    exec.foreach { exec =>
      if (convertSparkPlan(exec).isInstanceOf[NativeSupports]) {
        exec.setTagValue(convertibleTag, true)
      } else {
        exec.setTagValue(convertibleTag, false)
        exec.setTagValue(convertStrategyTag, NeverConvert)
      }
    }

    // execute some special strategies
    if (neverConvertSkewJoinEnabled)
      neverConvertSkewJoin(exec)
    if (neverConvertExecWithTimestampEnabled)
      neverConvertExecWithTimestamp(exec)
    if (neverConvertJoinsWithPostConditionEnabled)
      neverConvertJoinsWithPostCondition(exec)
    if (alwaysConvertDirectSortMergeJoinEnabled)
      alwaysConvertDirectSortMergeJoin(exec)
    if (neverConvertContinuousCodegensEnabled)
      neverConvertContinuousCodegens(exec)
    if (neverConvertScanWithInconvertibleChildrenEnabled)
      neverConvertScanWithInconvertibleChildren(exec)
    if (neverConvertAggregatesChildrenEnabled)
      neverConvertAggregatesChildren(exec)
    if (neverConvertPartialAggregateShuffleExchangeEnabled)
      neverConvertPartialAggregateShuffleExchange(exec)

    def hasMoreInconvertibleChildren(e: SparkPlan) =
      e.children.count(isNeverConvert) > e.children.count(isAlwaysConvert)

    // decide convert strategy by user preference
    exec.foreachUp {
      case exec if isNeverConvert(exec) =>
      case exec if isAlwaysConvert(exec) =>
      case e: ShuffleExchangeExec =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: BroadcastExchangeExec =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: FileSourceScanExec =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: ProjectExec if isAlwaysConvert(e.child) || preferNativeProject =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: FilterExec if isAlwaysConvert(e.child) || preferNativeFilter =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: SortExec if isAlwaysConvert(e.child) || preferNativeSort =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: UnionExec =>
        if (!hasMoreInconvertibleChildren(e)) {
          e.setTagValue(convertStrategyTag, AlwaysConvert)
        }
      case e: SortMergeJoinExec =>
        if (!hasMoreInconvertibleChildren(e) || preferNativeSmj) {
          e.setTagValue(convertStrategyTag, AlwaysConvert)
        }
      case e: BroadcastHashJoinExec =>
        if (!hasMoreInconvertibleChildren(e) || preferNativeBhj) {
          e.setTagValue(convertStrategyTag, AlwaysConvert)
        }
      case e: LocalLimitExec if isAlwaysConvert(e.child) || preferNativeLocalLimit =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: GlobalLimitExec if isAlwaysConvert(e.child) || preferNativeGlobalLimit =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: HashAggregateExec if isAlwaysConvert(e.child) || preferNativeAggr =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)

      case e =>
        // not marked -- default to NeverConvert
        e.setTagValue(convertStrategyTag, NeverConvert)
    }
  }

  def isNeverConvert(exec: SparkPlan): Boolean = {
    exec.getTagValue(convertStrategyTag).contains(NeverConvert)
  }

  def isAlwaysConvert(exec: SparkPlan): Boolean = {
    exec.getTagValue(convertStrategyTag).contains(AlwaysConvert)
  }

  private def neverConvertSkewJoin(exec: SparkPlan): Unit = {
    // exec.foreach {
    //   case e: SortMergeJoinExec if e.isSkewJoin =>
    //     e.setTagValue(convertStrategyTag, NeverConvert)
    //     e.children.foreach(_.setTagValue(convertStrategyTag, NeverConvert))
    //   case _ =>
    // }
  }

  private def neverConvertExecWithTimestamp(exec: SparkPlan): Unit = {
    exec.foreach {
      case e
          if e.output.exists(_.dataType == TimestampType) || e.children.exists(
            _.output.exists(_.dataType == TimestampType)) =>
        e.setTagValue(convertStrategyTag, NeverConvert)
      case _ =>
    }
  }

  private def neverConvertJoinsWithPostCondition(exec: SparkPlan): Unit = {
    exec.foreach {
      case e: SortMergeJoinExec if e.condition.nonEmpty =>
        e.setTagValue(convertStrategyTag, NeverConvert)
      case e: BroadcastHashJoinExec if e.condition.nonEmpty =>
        e.setTagValue(convertStrategyTag, NeverConvert)
      case _ =>
    }
  }

  private def alwaysConvertDirectSortMergeJoin(exec: SparkPlan): Unit = {
    exec.foreach {
      case e: SortMergeJoinExec if /* !e.isSkewJoin && */ !isNeverConvert(e) =>
        if (e.children.forall(child => {
            isSuccssorOfExchange(child) && !isNeverConvert(child)
          })) {
          e.setTagValue(convertStrategyTag, AlwaysConvert)
          e.children.foreach(_.setTagValue(convertStrategyTag, AlwaysConvert))
        }
      case _ =>
    }
  }

  def neverConvertContinuousCodegens(exec: SparkPlan): Unit = {
    val flag = TreeNodeTag[Int]("blaze.continuous.codegen")

    def isContinuousCodegen(exec: SparkPlan): Boolean =
      !isAlwaysConvert(exec) && exec.isInstanceOf[CodegenSupport]

    // fill
    exec.foreachUp {
      case exec if isContinuousCodegen(exec) =>
        val max = exec.children.flatMap(_.getTagValue(flag)) match {
          case xs if xs.nonEmpty => xs.max
          case _ => 0
        }
        exec.setTagValue(flag, max + 1)
      case _ =>
    }
    exec.foreach {
      case exec if isContinuousCodegen(exec) =>
        exec.children.filter(isContinuousCodegen).foreach { child =>
          child.setTagValue(flag, (exec.getTagValue(flag) ++ child.getTagValue(flag)).max)
        }
      case _ =>
    }

    // mark as never convert
    exec.foreach {
      case e if e.getTagValue(flag).exists(_ >= continuousCodegenThreshold) =>
        e.setTagValue(convertStrategyTag, NeverConvert)
      case _ =>
    }
  }

  private def neverConvertScanWithInconvertibleChildren(exec: SparkPlan): Unit = {
    exec.foreach {
      case e if isNeverConvert(e) && e.children.exists(_.isInstanceOf[FileSourceScanExec]) =>
        e.children.foreach {
          case child: FileSourceScanExec =>
            child.setTagValue(convertStrategyTag, NeverConvert)
          case _ =>
        }
      case _ =>
    }
  }
  private def neverConvertAggregatesChildren(exec: SparkPlan): Unit = {
    val aggrAheadFlag = TreeNodeTag[Boolean](name = "blaze.aggregates.children")
    def needPutFlag(exec: SparkPlan): Boolean = {
      if (isNeverConvert(exec) || isAlwaysConvert(exec)) {
        return false
      } else {
        return true
      }
    }
    exec.foreach {
      case exec: HashAggregateExec if isNeverConvert(exec) => {
        exec.children.foreach {
          case child if needPutFlag(child) =>
            child.setTagValue(aggrAheadFlag, true)
          case _ =>
        }
      }
      case f if f.getTagValue(aggrAheadFlag).contains(true) =>
        f.children.foreach {
          case child if needPutFlag(child) =>
            child.setTagValue(aggrAheadFlag, true)
          case _ =>
        }
      case _ =>
    }
    exec.foreach {
      case e if e.getTagValue(aggrAheadFlag).contains(true) =>
        e.setTagValue(convertStrategyTag, NeverConvert)
      case _ =>
    }
  }

  private def neverConvertPartialAggregateShuffleExchange(exec: SparkPlan): Unit = {
    exec.foreach {
      case exec: ShuffleExchangeExec
          if exec.child.isInstanceOf[HashAggregateExec] &&
            exec.child
              .asInstanceOf[HashAggregateExec]
              .aggregateExpressions
              .exists(_.mode == Partial) =>
        exec.setTagValue(convertStrategyTag, NeverConvert)
      case _ =>
    }
  }

  private def isSuccssorOfExchange(exec: SparkPlan): Boolean = {
    exec.children.forall(child => {
      child.isInstanceOf[Exchange] ||
        child.isInstanceOf[ShuffleQueryStageInput] ||
        child.isInstanceOf[SkewedShuffleQueryStageInput] ||
        child.isInstanceOf[QueryStageExec]
    })
  }
}

sealed trait ConvertStrategy {}
case object Default extends ConvertStrategy
case object AlwaysConvert extends ConvertStrategy
case object NeverConvert extends ConvertStrategy
