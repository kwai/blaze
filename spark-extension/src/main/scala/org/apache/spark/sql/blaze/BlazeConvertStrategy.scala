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
import org.apache.spark.sql.execution.aggregate.{
  HashAggregateExec,
  ObjectHashAggregateExec,
  SortAggregateExec
}
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.execution.GlobalLimitExec
import org.apache.spark.sql.execution.LocalLimitExec
import org.apache.spark.sql.execution.blaze.plan.NativeHashAggregateExec
import org.apache.spark.sql.execution.ExpandExec
import org.apache.spark.sql.execution.TakeOrderedAndProjectExec
import org.apache.spark.sql.types.TimestampType

object BlazeConvertStrategy extends Logging {
  import BlazeConverters._

  val depthTag: TreeNodeTag[Int] = TreeNodeTag("blaze.depth")
  val convertibleTag: TreeNodeTag[Boolean] = TreeNodeTag("blaze.convertible")
  val convertStrategyTag: TreeNodeTag[ConvertStrategy] = TreeNodeTag("blaze.convert.strategy")
  val hashAggrModeTag: TreeNodeTag[AggregateMode] = TreeNodeTag("blaze.hash.aggr.mode")

  def apply(exec: SparkPlan): Unit = {
    exec.foreach(_.setTagValue(convertibleTag, true))
    exec.foreach(_.setTagValue(convertStrategyTag, Default))

    // fill depth
    exec.setTagValue(depthTag, 0)
    exec.foreach(e => {
      val childDepth = e.getTagValue(depthTag).get + 1
      e.children.foreach(_.setTagValue(depthTag, childDepth))
    })

    // try to convert all plans and fill convertible tag back to origin exec
    var danglingChildren = Seq[SparkPlan]()
    exec.foreachUp { exec =>
      val (newDangling, children) =
        danglingChildren.splitAt(danglingChildren.length - exec.children.length)

      val converted = convertSparkPlan(exec.withNewChildren(children))
      converted match {
        case e if NativeHelper.isNative(e) =>
          exec.setTagValue(convertibleTag, true)

          // set aggregation mode
          exec match {
            case e: HashAggregateExec =>
              exec.setTagValue(
                hashAggrModeTag,
                NativeHashAggregateExec.getAggrMode(
                  e.aggregateExpressions,
                  e.requiredChildDistributionExpressions))
            case _ =>
          }

        case _ =>
          exec.setTagValue(convertibleTag, false)
          exec.setTagValue(convertStrategyTag, NeverConvert)
      }
      danglingChildren = newDangling :+ converted
    }

    // fill convert strategy of stage inputs
    // get stageInput from Shim trait
    exec.foreachUp {
      case stageInput if Shims.get.sparkPlanShims.isQueryStageInput(stageInput) =>
        stageInput.setTagValue(
          convertStrategyTag,
          if (NativeHelper.isNative(stageInput)) {
            AlwaysConvert
          } else {
            NeverConvert
          })
      case _ =>
    }

    // execute some special strategies
    removeInefficientConverts(exec)

    exec.foreachUp {
      case exec if isNeverConvert(exec) || isAlwaysConvert(exec) =>
      // already decided, do nothing
      case e: ShuffleExchangeExec if isAlwaysConvert(e.child) =>
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
      case e: UnionExec
          if e.children.count(isAlwaysConvert) >= e.children.count(isNeverConvert) =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: SortMergeJoinExec if e.children.exists(isAlwaysConvert) =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: BroadcastHashJoinExec if e.children.forall(isAlwaysConvert) =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: LocalLimitExec if isAlwaysConvert(e.child) =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: GlobalLimitExec if isAlwaysConvert(e.child) =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: TakeOrderedAndProjectExec if isAlwaysConvert(e.child) =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: HashAggregateExec
          if isAlwaysConvert(e.child) || !e.getTagValue(hashAggrModeTag).contains(Partial) =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: ExpandExec if isAlwaysConvert(e.child) =>
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

  private def removeInefficientConverts(exec: SparkPlan): Unit = {
    var finished = false

    while (!finished) {
      finished = true
      val dontConvertIf = (exec: SparkPlan, condition: Boolean) => {
        if (condition) {
          exec.setTagValue(convertStrategyTag, NeverConvert)
          finished = false
        }
      }

      exec.foreach { e =>
        // [ (non-native) -> Filter(native) ]
        dontConvertIf(
          e,
          e.isInstanceOf[FilterExec] && !isNeverConvert(e) && isNeverConvert(e.children.head))

        // [ (non-native) -> PartialAggr(native) ]
        dontConvertIf(
          e,
          e.isInstanceOf[HashAggregateExec] &&
            isPartialHashAggregate(e) &&
            !isNeverConvert(e) &&
            isNeverConvert(e.children.head))

        // [ HashAggr/ObjectAggr/SortAggr -> ShuffleExchange(native) ]
        dontConvertIf(
          e,
          e.isInstanceOf[ShuffleExchangeExec] &&
            isAggregate(e.children.head) &&
            !isNeverConvert(e) &&
            isNeverConvert(e.children.head))

        // [ NativeParquetScan -> (non-native) ]
        e.children.foreach { child =>
          dontConvertIf(
            child,
            child.isInstanceOf[FileSourceScanExec] &&
              !isNeverConvert(child) &&
              isNeverConvert(e))
        }
        // [ aggregateExpressions > threshold ]
        dontConvertIf(
          e,
          e.isInstanceOf[HashAggregateExec] &&
            isPartialHashAggregate(e) &&
            !isNeverConvert(e) &&
            e.asInstanceOf[HashAggregateExec].aggregateExpressions.length > 5)
      }
    }
  }

  private def isPartialHashAggregate(e: SparkPlan): Boolean = {
    e match {
      case e: HashAggregateExec => e.requiredChildDistributionExpressions.isEmpty
      case _ => false
    }
  }
  private def isAggregate(e: SparkPlan): Boolean = {
    e match {
      case e: HashAggregateExec => e.requiredChildDistributionExpressions.isEmpty
      case _: ObjectHashAggregateExec => true
      case _: SortAggregateExec => true
      case _ => false
    }
  }
}

sealed trait ConvertStrategy {}
case object Default extends ConvertStrategy
case object AlwaysConvert extends ConvertStrategy
case object NeverConvert extends ConvertStrategy
