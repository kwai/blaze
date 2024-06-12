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
import org.apache.spark.sql.catalyst.optimizer.BuildLeft
import org.apache.spark.sql.catalyst.optimizer.BuildRight
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec
import org.apache.spark.sql.execution.adaptive.InvalidAQEPlanException
import org.apache.spark.sql.execution.blaze.plan.NativeRenameColumnsExec
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.execution.joins.BroadcastNestedLoopJoinExec
import org.apache.spark.sql.execution.joins.blaze.plan.NativeBroadcastJoinExec

object InterceptedValidateSparkPlan extends Logging {

  def validate(plan: SparkPlan): Unit = {
    plan match {
      case b: BroadcastHashJoinExec =>
        val (buildPlan, probePlan) = b.buildSide match {
          case BuildLeft => (b.left, b.right)
          case BuildRight => (b.right, b.left)
        }
        if (!buildPlan.isInstanceOf[BroadcastQueryStageExec]) {
          validate(buildPlan)
        }
        validate(probePlan)

      case b: NativeBroadcastJoinExec => // same as non-native BHJ
        var (buildPlan, probePlan) = b.buildSide match {
          case BuildLeft => (b.left, b.right)
          case BuildRight => (b.right, b.left)
        }
        if (buildPlan.isInstanceOf[NativeRenameColumnsExec]) {
          buildPlan = buildPlan.children.head
        }
        if (!buildPlan.isInstanceOf[BroadcastQueryStageExec]) {
          validate(buildPlan)
        }
        validate(probePlan)

      case b: BroadcastNestedLoopJoinExec =>
        val (buildPlan, probePlan) = b.buildSide match {
          case BuildLeft => (b.left, b.right)
          case BuildRight => (b.right, b.left)
        }
        if (!buildPlan.isInstanceOf[BroadcastQueryStageExec]) {
          validate(buildPlan)
        }
        validate(probePlan)
      case q: BroadcastQueryStageExec => errorOnInvalidBroadcastQueryStage(q)
      case _ => plan.children.foreach(validate)
    }
  }

  private def errorOnInvalidBroadcastQueryStage(plan: SparkPlan): Unit = {
    throw InvalidAQEPlanException("Invalid broadcast query stage", plan)
  }
}
