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

import org.blaze.sparkverEnableMembers

@sparkverEnableMembers("3.5")
class BlazeAdaptiveQueryExecSuite
    extends org.apache.spark.sql.QueryTest
    with BaseBlazeSQLSuite
    with org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper {

  import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
  import org.apache.spark.sql.execution.{PartialReducerPartitionSpec, SparkPlan}
  import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, AdaptiveSparkPlanExec}
  import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
  import org.apache.spark.sql.execution.exchange.Exchange
  import org.apache.spark.sql.execution.ui.{SparkListenerSQLAdaptiveExecutionUpdate, SparkListenerSQLAdaptiveSQLMetricUpdates}
  import org.apache.spark.sql.internal.SQLConf
  import org.apache.spark.sql.test.SQLTestData.TestData

  import testImplicits._

  // Copy from spark/sql/core/src/test/scala/org/apache/spark/sql/execution/adaptive/AdaptiveQueryExecSuite.scala
  test("SPARK-35725: Support optimize skewed partitions in RebalancePartitions") {
    withTempView("v") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "true",
        SQLConf.ADAPTIVE_OPTIMIZE_SKEWS_IN_REBALANCE_PARTITIONS_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.SHUFFLE_PARTITIONS.key -> "5",
        SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key -> "1") {

        spark.sparkContext
          .parallelize((1 to 10).map(i => TestData(if (i > 4) 5 else i, i.toString)), 3)
          .toDF("c1", "c2")
          .createOrReplaceTempView("v")

        def checkPartitionNumber(
            query: String,
            skewedPartitionNumber: Int,
            totalNumber: Int): Unit = {
          val (_, adaptive) = runAdaptiveAndVerifyResult(query)
          val read = collect(adaptive) { case read: AQEShuffleReadExec =>
            read
          }
          assert(read.size == 1)
          assert(
            read.head.partitionSpecs.count(_.isInstanceOf[PartialReducerPartitionSpec]) ==
              skewedPartitionNumber)
          assert(read.head.partitionSpecs.size == totalNumber)
        }

        withSQLConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "100") {
          // partition size [0, 75, 45, 68, 34]
          checkPartitionNumber("SELECT /*+ REBALANCE(c1) */ * FROM v", 2, 4)
          checkPartitionNumber("SELECT /*+ REBALANCE */ * FROM v", 0, 3)
        }

        // no skewed partition should be optimized
        withSQLConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "10000") {
          checkPartitionNumber("SELECT /*+ REBALANCE(c1) */ * FROM v", 0, 1)
        }
      }
    }
  }

  private def runAdaptiveAndVerifyResult(query: String): (SparkPlan, SparkPlan) = {
    var finalPlanCnt = 0
    var hasMetricsEvent = false
    val listener = new SparkListener {
      override def onOtherEvent(event: SparkListenerEvent): Unit = {
        event match {
          case SparkListenerSQLAdaptiveExecutionUpdate(_, _, sparkPlanInfo) =>
            if (sparkPlanInfo.simpleString.startsWith("AdaptiveSparkPlan isFinalPlan=true")) {
              finalPlanCnt += 1
            }
          case _: SparkListenerSQLAdaptiveSQLMetricUpdates =>
            hasMetricsEvent = true
          case _ => // ignore other events
        }
      }
    }
    spark.sparkContext.addSparkListener(listener)

    val dfAdaptive = sql(query)
    val planBefore = dfAdaptive.queryExecution.executedPlan
    assert(planBefore.toString.startsWith("AdaptiveSparkPlan isFinalPlan=false"))
    val result = dfAdaptive.collect()
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = sql(query)
      checkAnswer(df, result)
    }
    val planAfter = dfAdaptive.queryExecution.executedPlan
    assert(planAfter.toString.startsWith("AdaptiveSparkPlan isFinalPlan=true"))
    val adaptivePlan = planAfter.asInstanceOf[AdaptiveSparkPlanExec].executedPlan

    spark.sparkContext.listenerBus.waitUntilEmpty()
    // AQE will post `SparkListenerSQLAdaptiveExecutionUpdate` twice in case of subqueries that
    // exist out of query stages.
    val expectedFinalPlanCnt = adaptivePlan.find(_.subqueries.nonEmpty).map(_ => 2).getOrElse(1)
    assert(finalPlanCnt == expectedFinalPlanCnt)
    spark.sparkContext.removeSparkListener(listener)

    val expectedMetrics = findInMemoryTable(planAfter).nonEmpty ||
      subqueriesAll(planAfter).nonEmpty
    assert(hasMetricsEvent == expectedMetrics)

    val exchanges = adaptivePlan.collect { case e: Exchange =>
      e
    }
    assert(exchanges.isEmpty, "The final plan should not contain any Exchange node.")
    (dfAdaptive.queryExecution.sparkPlan, adaptivePlan)
  }

  private def findInMemoryTable(plan: SparkPlan): Seq[InMemoryTableScanExec] = {
    collect(plan) {
      case c: InMemoryTableScanExec
          if c.relation.cachedPlan.isInstanceOf[AdaptiveSparkPlanExec] =>
        c
    }
  }
}
