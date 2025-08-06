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
package org.apache.spark.sql.hive.blaze

import org.apache.spark.internal.Logging
import org.apache.spark.sql.blaze.BlazeConverters.addRenameColumnsExec
import org.apache.spark.sql.blaze.util.BlazeLogUtils.logPlanConversion
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.hive.execution.HiveTableScanExec
import org.apache.spark.sql.hive.execution.blaze.plan.NativePaimonTableScanExec

object BlazeHiveConverters extends Logging {

  def isNativePaimonTableScan(exec: SparkPlan): Boolean = {
    exec match {
      case e: HiveTableScanExec
          if e.relation.tableMeta.storage.serde.isDefined
            && e.relation.tableMeta.storage.serde.get.contains("Paimon") =>
        true
      case _ => false
    }
  }

  def convertPaimonTableScanExec(exec: SparkPlan): SparkPlan = {
    val hiveExec = exec.asInstanceOf[HiveTableScanExec]
    assert(
      PaimonUtil.isPaimonCowTable(
        PaimonUtil.loadTable(hiveExec.relation.tableMeta.location.toString)),
      "paimon MOR/MOW mode is not supported")
    val (relation, output, requestedAttributes, partitionPruningPred) = (
      hiveExec.relation,
      hiveExec.output,
      hiveExec.requestedAttributes,
      hiveExec.partitionPruningPred)
    logPlanConversion(
      exec,
      "relation" -> relation.getClass,
      "relation.location" -> relation.tableMeta.location,
      "output" -> output,
      "requestedAttributes" -> requestedAttributes,
      "partitionPruningPred" -> partitionPruningPred)

    addRenameColumnsExec(NativePaimonTableScanExec(hiveExec))
  }
}
