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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateMode
import org.apache.spark.sql.catalyst.expressions.aggregate.Partial
import org.apache.spark.sql.execution.FilterExec
import org.apache.spark.sql.execution.SortExec
import org.apache.spark.sql.execution.UnionExec
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.execution.GlobalLimitExec
import org.apache.spark.sql.execution.LocalLimitExec
import org.apache.spark.sql.execution.adaptive.QueryStageInput
import org.apache.spark.sql.execution.blaze.plan.NativeHashAggregateExec
import org.apache.spark.sql.types.TimestampType

object BlazeConvertStrategy extends Logging {
  import BlazeConverters._

  val neverConvertExecWithTimestampEnabled: Boolean =
    SparkEnv.get.conf
      .getBoolean(
        "spark.blaze.strategy.enable.neverConvertExecWithTimestamp",
        defaultValue = true)
  val neverConvertJoinsWithPostConditionEnabled: Boolean =
    SparkEnv.get.conf.getBoolean(
      "spark.blaze.strategy.enable.neverConvertJoinsWithCondition",
      defaultValue = true)
  val neverConvertPartialAggregateShuffleExchangeEnabled: Boolean =
    SparkEnv.get.conf.getBoolean(
      "spark.blaze.strategy.enable.neverConvertAggregateShuffleExchange",
      defaultValue = true)

  val convertibleTag: TreeNodeTag[Boolean] = TreeNodeTag("blaze.convertible")
  val convertStrategyTag: TreeNodeTag[ConvertStrategy] = TreeNodeTag("blaze.convert.strategy")
  val hashAggrModeTag: TreeNodeTag[AggregateMode] = TreeNodeTag("blaze.hash.aggr.mode")

  def apply(exec: SparkPlan): Unit = {
    exec.foreach(_.setTagValue(convertibleTag, true))
    exec.foreach(_.setTagValue(convertStrategyTag, Default))

    // try to convert all plans and fill convertible tag back to origin exec
    var danglingChildren = Seq[SparkPlan]()
    exec.foreachUp { exec =>
      val (newDangling, children) =
        danglingChildren.splitAt(danglingChildren.length - exec.children.length)

      val converted = convertSparkPlan(exec.withNewChildren(children))
      converted match {
        case e: QueryStageInput if NativeSupports.isNative(e) =>
          exec.setTagValue(convertibleTag, true)

        case e: NativeHashAggregateExec =>
          exec.setTagValue(hashAggrModeTag, e.aggrMode)
          exec.setTagValue(convertibleTag, true)

        case _: NativeSupports =>
          exec.setTagValue(convertibleTag, true)

        case _ =>
          exec.setTagValue(convertibleTag, false)
          exec.setTagValue(convertStrategyTag, NeverConvert)
      }
      danglingChildren = newDangling :+ converted
    }

    // execute some special strategies
    if (neverConvertExecWithTimestampEnabled) {
      neverConvertExecWithTimestamp(exec)
    }
    if (neverConvertJoinsWithPostConditionEnabled) {
      neverConvertJoinsWithPostCondition(exec)
    }
    if (neverConvertPartialAggregateShuffleExchangeEnabled) {
      neverConvertPartialAggregateShuffleExchange(exec)
    }
    removeInefficientConverts(exec)

    def hasLessConvertibleChildren(e: SparkPlan) =
      e.children.count(isNeverConvert) > e.children.count(isAlwaysConvert)

    exec.foreachUp {
      case exec if isNeverConvert(exec) || isAlwaysConvert(exec) =>
      // already decided, do nothing
      case e: ShuffleExchangeExec =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: BroadcastExchangeExec =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: FileSourceScanExec =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: ProjectExec if isAlwaysConvert(e.child) =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: FilterExec if isAlwaysConvert(e.child) =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: SortExec if isAlwaysConvert(e.child) =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: UnionExec if !hasLessConvertibleChildren(e) =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: SortMergeJoinExec if !hasLessConvertibleChildren(e) =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: BroadcastHashJoinExec if !hasLessConvertibleChildren(e) =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: LocalLimitExec if isAlwaysConvert(e.child) =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: GlobalLimitExec if isAlwaysConvert(e.child) =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: HashAggregateExec
          if isAlwaysConvert(e.child) || !e.getTagValue(hashAggrModeTag).contains(Partial) =>
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

  private def neverConvertPartialAggregateShuffleExchange(exec: SparkPlan): Unit = {
    exec.foreach {
      case exec @ ShuffleExchangeExec(_, aggr @ HashAggregateExec(None, _, _, _, _, _, _))
          if isNeverConvert(aggr) =>
        // shuffle of partial hash aggregate without requiring child distribution
        exec.setTagValue(convertStrategyTag, NeverConvert)

      case _ =>
    }
  }

  private def removeInefficientConverts(exec: SparkPlan): Unit = {
    var finished = false

    while (!finished) {
      finished = true

      exec.foreach {
        // [ ConvertToNative -> NativeFilter ]
        case e: FilterExec if !isNeverConvert(e) && isNeverConvert(e.child) =>
          e.setTagValue(convertStrategyTag, NeverConvert)
          finished = false

        // [ ConvertToNative -> NativePartialAggr ]
        case e: HashAggregateExec if !isNeverConvert(e) && isNeverConvert(e.child) =>
          if (e.getTagValue(hashAggrModeTag).contains(Partial)) {
            e.setTagValue(convertStrategyTag, NeverConvert)
            finished = false
          }

        case e if isNeverConvert(e) =>
          e.children.foreach {
            // [ NativeParquetScan -> ConvertToUnsafeRow ]
            case child: FileSourceScanExec if !isNeverConvert(child) =>
              child.setTagValue(convertStrategyTag, NeverConvert)
              finished = false

            case _ =>
          }

        case _ =>
      }
    }
  }
}

sealed trait ConvertStrategy {}
case object Default extends ConvertStrategy
case object AlwaysConvert extends ConvertStrategy
case object NeverConvert extends ConvertStrategy
