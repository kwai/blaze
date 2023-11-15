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
import org.apache.spark.sql.execution.FilterExec
import org.apache.spark.sql.execution.SortExec
import org.apache.spark.sql.execution.UnionExec
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.execution.ExpandExec
import org.apache.spark.sql.execution.GlobalLimitExec
import org.apache.spark.sql.execution.LocalLimitExec
import org.apache.spark.sql.execution.TakeOrderedAndProjectExec
import org.apache.spark.sql.execution.aggregate.ObjectHashAggregateExec
import org.apache.spark.sql.execution.aggregate.SortAggregateExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.execution.GenerateExec
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.joins.BroadcastNestedLoopJoinExec

object BlazeConvertStrategy extends Logging {
  import BlazeConverters._

  val convertibleTag: TreeNodeTag[Boolean] = TreeNodeTag("blaze.convertible")
  val convertStrategyTag: TreeNodeTag[ConvertStrategy] = TreeNodeTag("blaze.convert.strategy")

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
        case e if NativeHelper.isNative(e) || e.getTagValue(convertibleTag).contains(true) =>
          exec.setTagValue(convertibleTag, true)
        case _ =>
          exec.setTagValue(convertibleTag, false)
          exec.setTagValue(convertStrategyTag, NeverConvert)
      }
      danglingChildren = newDangling :+ converted
    }

    // fill convert strategy of stage inputs
    exec.foreachUp {
      case e if !e.isInstanceOf[NativeSupports] && NativeHelper.isNative(e) =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case _ =>
    }

    // execute some special strategies
    removeInefficientConverts(exec)

    exec.foreachUp {
      case exec if isNeverConvert(exec) || isAlwaysConvert(exec) =>
      // already decided, do nothing
      case e: ShuffleExchangeExec if isAlwaysConvert(e.child) || !isAggregate(e.child) =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: BroadcastExchangeExec =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: FileSourceScanExec =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: BatchScanExec =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: ProjectExec if isAlwaysConvert(e.child) =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: FilterExec if isAlwaysConvert(e.child) =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: SortExec => // prefer native sort even if child is non-native
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: UnionExec
          if e.children.count(isAlwaysConvert) >= e.children.count(isNeverConvert) =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: SortMergeJoinExec if e.children.exists(isAlwaysConvert) =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: BroadcastHashJoinExec if e.children.forall(isAlwaysConvert) =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: BroadcastNestedLoopJoinExec if e.children.forall(isAlwaysConvert) =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: LocalLimitExec if isAlwaysConvert(e.child) =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: GlobalLimitExec if isAlwaysConvert(e.child) =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: TakeOrderedAndProjectExec if isAlwaysConvert(e.child) =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: HashAggregateExec if isAlwaysConvert(e.child) =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: SortAggregateExec if isAlwaysConvert(e.child) =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: ExpandExec if isAlwaysConvert(e.child) =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: WindowExec if isAlwaysConvert(e.child) =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: GenerateExec if isAlwaysConvert(e.child) =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: ObjectHashAggregateExec if isAlwaysConvert(e.child) =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: LocalTableScanExec =>
        e.setTagValue(convertStrategyTag, AlwaysConvert)
      case e: DataWritingCommandExec if isAlwaysConvert(e.child) =>
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
        // NonNative -> NativeFilter
        // don't use NativeFilter because it requires ConvertToNative with a lot of records
        if (!isNeverConvert(e) && e.isInstanceOf[FilterExec]) {
          val child = e.children.head
          dontConvertIf(e, isNeverConvert(child))
        }

        // NonNative -> NativeAgg
        // don't use NativeAgg because it requires ConvertToNative with a lot of records
        if (!isNeverConvert(e) && isAggregate(e)) {
          val child = e.children.head
          dontConvertIf(e, isNeverConvert(child))
        }

        // Agg -> NativeShuffle
        // don't use NativeShuffle because the next stage is like to use non-native shuffle reader
        if (!isNeverConvert(e) && e.isInstanceOf[ShuffleExchangeLike]) {
          val child = e.children.head
          dontConvertIf(e, isAggregate(child) && isNeverConvert(child))
        }

        // NativeExpand -> NonNative
        // don't use NativeExpand because it requires C2R with a lot of records
        if (isNeverConvert(e)) {
          e.children.find(_.isInstanceOf[ExpandExec]) match {
            case Some(expand) => dontConvertIf(expand, !isNeverConvert(expand))
            case _ =>
          }
        }

        // NativeParquetScan -> NonNative
        // don't use NativeParquetScan because it requires C2R with a lot of records
        if (isNeverConvert(e)) {
          e.children.find(_.isInstanceOf[FileSourceScanExec]) match {
            case Some(scan) => dontConvertIf(scan, !isNeverConvert(scan))
            case _ =>
          }
        }

        // NonNative -> NativeSort -> NonNative
        // don't use native sort
        if (isNeverConvert(e)) {
          e.children.filter(_.isInstanceOf[SortExec]).foreach { sort =>
            dontConvertIf(sort, !isNeverConvert(sort) && isNeverConvert(sort.children.head))
          }
        }
      }
    }
  }

  private def isAggregate(e: SparkPlan): Boolean = {
    e.isInstanceOf[HashAggregateExec] ||
    e.isInstanceOf[SortAggregateExec] ||
    e.isInstanceOf[ObjectHashAggregateExec]
  }
}

sealed trait ConvertStrategy {}
case object Default extends ConvertStrategy
case object AlwaysConvert extends ConvertStrategy
case object NeverConvert extends ConvertStrategy
