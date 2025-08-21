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
package org.apache.spark.sql.auron

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.SparkPlan

import org.apache.auron.sparkver

object InterceptedValidateSparkPlan extends Logging {

  @sparkver("3.2 / 3.3 / 3.4 / 3.5")
  def validate(plan: SparkPlan): Unit = {
    import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec
    import org.apache.spark.sql.execution.auron.plan.NativeRenameColumnsBase
    import org.apache.spark.sql.execution.joins.BroadcastNestedLoopJoinExec
    import org.apache.spark.sql.execution.joins.auron.plan.NativeBroadcastJoinExec
    import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
    import org.apache.spark.sql.catalyst.optimizer.BuildLeft
    import org.apache.spark.sql.catalyst.optimizer.BuildRight

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
        if (buildPlan.isInstanceOf[NativeRenameColumnsBase]) {
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

  @sparkver("3.0 / 3.1")
  def validate(plan: SparkPlan): Unit = {
    throw new UnsupportedOperationException("validate is not supported in spark 3.0.3 or 3.1.3")
  }

  @sparkver("3.2 / 3.3 / 3.4 / 3.5")
  private def errorOnInvalidBroadcastQueryStage(plan: SparkPlan): Unit = {
    import org.apache.spark.sql.execution.adaptive.InvalidAQEPlanException
    throw InvalidAQEPlanException("Invalid broadcast query stage", plan)
  }
}
