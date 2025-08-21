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
package org.apache.spark.sql.hive.auron

import org.apache.spark.internal.Logging
import org.apache.spark.sql.auron.AuronConverters
import org.apache.spark.sql.auron.AuronConvertProvider
import org.apache.spark.sql.auron.util.AuronLogUtils
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.hive.execution.HiveTableScanExec
import org.apache.spark.sql.hive.execution.auron.plan.NativePaimonTableScanExec

class PaimonConvertProvider extends AuronConvertProvider with Logging {

  override def isEnabled: Boolean = {
    AuronConverters.getBooleanConf("spark.auron.enable.paimon.scan", defaultValue = false)
  }

  override def isSupported(exec: SparkPlan): Boolean = {
    exec match {
      case e: HiveTableScanExec
          if e.relation.tableMeta.storage.serde.isDefined
            && e.relation.tableMeta.storage.serde.get.contains("Paimon") =>
        true
      case _ => false
    }
  }

  override def convert(exec: SparkPlan): SparkPlan = {
    exec match {
      case hiveExec: HiveTableScanExec => convertPaimonTableScanExec(hiveExec)
      case _ => exec
    }
  }

  private def convertPaimonTableScanExec(exec: HiveTableScanExec): SparkPlan = {
    assert(
      PaimonUtil.isPaimonCowTable(
        PaimonUtil.loadTable(exec.relation.tableMeta.location.toString)),
      "paimon MOR/MOW mode is not supported")
    val (relation, output, requestedAttributes, partitionPruningPred) =
      (exec.relation, exec.output, exec.requestedAttributes, exec.partitionPruningPred)
    AuronLogUtils.logDebugPlanConversion(
      exec,
      Seq(
        "relation" -> relation.getClass,
        "relation.location" -> relation.tableMeta.location,
        "output" -> output,
        "requestedAttributes" -> requestedAttributes,
        "partitionPruningPred" -> partitionPruningPred))

    AuronConverters.addRenameColumnsExec(NativePaimonTableScanExec(exec))
  }
}
