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
import org.apache.spark.internal.config.{ConfigEntry, OptionalConfigEntry}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.ColumnarRule
import org.apache.spark.sql.execution.blaze.plan.NativeTakeOrderedExec
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.internal.SQLConf

class BlazeSparkSessionExtension extends (SparkSessionExtensions => Unit) with Logging {
  import BlazeSparkSessionExtension._

  override def apply(extensions: SparkSessionExtensions): Unit = {
    SparkEnv.get.conf.set("spark.sql.adaptive.enabled", "true")
    SparkEnv.get.conf.set("spark.sql.adaptive.forceApply", "true")
    logInfo("org.apache.spark.BlazeSparkSessionExtension enabled")

    assert(BlazeSparkSessionExtension.blazeEnabledKey != null)

    extensions.injectOptimizerRule(sparkSession => {
      BlazeRuleEngine(sparkSession)
    })

    extensions.injectColumnar(sparkSession => {
      BlazeColumnarOverrides(sparkSession)
    })
  }
}

object BlazeSparkSessionExtension {
  lazy val blazeEnabledKey: ConfigEntry[Boolean] = SQLConf
    .buildConf("spark.blaze.enable")
    .booleanConf
    .createWithDefault(true)

  lazy val blazeMissPatterns: OptionalConfigEntry[String] = SQLConf
    .buildConf("spark.blaze.blazeMissPatterns")
    .stringConf
    .createOptional
}

case class BlazeColumnarOverrides(sparkSession: SparkSession) extends ColumnarRule with Logging {
  import BlazeSparkSessionExtension._

  override def preColumnarTransitions: Rule[SparkPlan] =
    new Rule[SparkPlan] {
      override def apply(sparkPlan: SparkPlan): SparkPlan = {
        if (!sparkPlan.conf.getConf(blazeEnabledKey)) {
          return sparkPlan // performs no conversion if blaze is not enabled
        }

        // generate convert strategy
        BlazeConvertStrategy.apply(sparkPlan)
        logDebug("Blaze convert strategy for current stage:")
        sparkPlan.foreach { exec =>
          val depth = exec.getTagValue(BlazeConvertStrategy.depthTag).getOrElse(0)
          val nodeName = exec.nodeName
          val convertible = exec
            .getTagValue(BlazeConvertStrategy.convertibleTag)
            .getOrElse(false)
          val strategy =
            exec.getTagValue(BlazeConvertStrategy.convertStrategyTag).getOrElse(Default)
          logWarning(s" +${"-" * depth} $nodeName (convertible=$convertible, strategy=$strategy)")
        }
        var sparkPlanTransformed = BlazeConverters.convertSparkPlanRecursively(sparkPlan)

        // wrap with ConvertUnsafeRowExec if top exec is native
        if (NativeHelper.isNative(sparkPlanTransformed)) {
          val topNative = NativeHelper.getUnderlyingNativePlan(sparkPlanTransformed)
          val topNeededConvertToUnsafeRow = topNative match {
            case _: Exchange | _: NativeTakeOrderedExec => false
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

case class BlazeRuleEngine(sparkSession: SparkSession) extends Rule[LogicalPlan] with Logging {
  import BlazeSparkSessionExtension._

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.foreachUp {
      case p @ LogicalRelation(fsRelation: HadoopFsRelation, _, _, _) =>
        if (!fsRelation.fileFormat.isInstanceOf[ParquetFileFormat]) {
          p.conf.setConf(blazeEnabledKey, false)
          sparkSession.sparkContext.conf
            .set(blazeMissPatterns, BlazeMissPatterns.NonParquetFormat)
        }

      case h: HiveTableRelation =>
        h.conf.setConf(blazeEnabledKey, false)
        sparkSession.sparkContext.conf
          .set(blazeMissPatterns, BlazeMissPatterns.NonParquetFormat)

      case _ =>
    }
    plan
  }

  object BlazeMissPatterns extends Enumeration {
    type BlazeMissPatterns = String
    val NonParquetFormat = "NonParquetFormat"
  }
}
