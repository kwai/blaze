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

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.ColumnarRule
import org.apache.spark.sql.execution.exchange.Exchange

class BlazeSparkSessionExtension extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    SparkEnv.get.conf.set("spark.sql.adaptive.enabled", "true")
    SparkEnv.get.conf.set("spark.sql.adaptive.forceApply", "true")
    logInfo("org.apache.spark.BlazeSparkSessionExtension enabled")

    extensions.injectColumnar(sparkSession => {
      BlazeColumnarOverrides(sparkSession)
    })
  }
}

case class BlazeColumnarOverrides(sparkSession: SparkSession) extends ColumnarRule with Logging {
  override def preColumnarTransitions: Rule[SparkPlan] =
    new Rule[SparkPlan] {
      override def apply(sparkPlan: SparkPlan): SparkPlan = {
        BlazeConvertStrategy.apply(sparkPlan)
        var sparkPlanTransformed = BlazeConverters.convertSparkPlanRecursively(sparkPlan)

        // wrap with ConvertUnsafeRowExec if top exec is native
        if (NativeSupports.isNative(sparkPlanTransformed)) {
          val topNative = NativeSupports.getUnderlyingNativePlan(sparkPlanTransformed)
          val topNeededConvertToUnsafeRow = topNative match {
            case _: Exchange => false
            case _ => true
          }
          if (topNeededConvertToUnsafeRow) {
            sparkPlanTransformed = BlazeConverters.convertToUnsafeRow(sparkPlanTransformed)
          }
        }

        logDebug(s"Transformed spark plan after preColumnarTransitions:\n${sparkPlanTransformed
          .treeString(verbose = true, addSuffix = true)}")
        sparkPlanTransformed
      }
    }
}
