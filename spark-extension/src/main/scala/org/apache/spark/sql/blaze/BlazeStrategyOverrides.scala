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
package org.apache.gluten.extension

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.blaze.BlazeConvertStrategy
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.blaze.plan.BuildLeft
import org.apache.spark.sql.execution.blaze.plan.BuildRight

case class BlazeStrategyOverrides(session: SparkSession) extends Strategy {
  private val planner = SparkSession.active.sessionState.planner

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    markSmallerJoinSide(plan) match {
      case Seq(sparkPlan) => return Seq(sparkPlan)
      case Nil =>
    }
    Nil
  }

  private def markSmallerJoinSide(plan: LogicalPlan): Seq[SparkPlan] = {
    planner.JoinSelection.apply(plan) match {
      case Seq(physicalPlan) if physicalPlan.children.length == 2 =>
        val left = plan.children(0)
        val right = plan.children(1)
        if (right.stats.sizeInBytes <= left.stats.sizeInBytes) {
          physicalPlan.setTagValue(BlazeConvertStrategy.joinSmallerSideTag, BuildRight)
        } else {
          physicalPlan.setTagValue(BlazeConvertStrategy.joinSmallerSideTag, BuildLeft)
        }
        Seq(physicalPlan)

      case Nil => Nil
    }
  }
}
