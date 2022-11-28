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

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.blaze.plan.ArrowBroadcastExchangeBase
import org.apache.spark.sql.execution.blaze.plan.ArrowShuffleExchangeBase
import org.apache.spark.storage.FileSegment

abstract class Shims {
  def rddShims: RDDShims
  def sparkPlanShims: SparkPlanShims
  def shuffleShims: ShuffleShims
  def broadcastShims: BroadcastShims
  def exprShims: ExprShims
}
object Shims {
  lazy val get: Shims = {
    // scalastyle:off throwerror
    throw new NotImplementedError("get not implemented")
    // scalastyle:on throwerror
  }
}

trait RDDShims {
  def getShuffleReadFull(rdd: RDD[_]): Boolean
  def setShuffleReadFull(rdd: RDD[_], shuffleReadFull: Boolean): Unit
}

trait SparkPlanShims {
  def isNative(plan: SparkPlan): Boolean
  def getUnderlyingNativePlan(plan: SparkPlan): NativeSupports
  def executeNative(plan: SparkPlan): NativeRDD

  def isQueryStageInput(plan: SparkPlan): Boolean
  def getChildStage(plan: SparkPlan): SparkPlan
}

trait ShuffleShims {
  def createArrowShuffleExchange(
      outputPartitioning: Partitioning,
      child: SparkPlan): ArrowShuffleExchangeBase

  def createFileSegment(file: File, offset: Long, length: Long, numRecords: Long): FileSegment
}

trait BroadcastShims {
  def createArrowBroadcastExchange(
      mode: BroadcastMode,
      child: SparkPlan): ArrowBroadcastExchangeBase
}

trait ExprShims {
  def getEscapeChar(expr: Expression): Char
  def getAggregateExpressionFilter(expr: Expression): Option[Expression]
}
