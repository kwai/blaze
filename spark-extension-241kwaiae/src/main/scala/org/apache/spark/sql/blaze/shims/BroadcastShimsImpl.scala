package org.apache.spark.sql.blaze.shims

import org.apache.spark.sql.blaze.BroadcastShims
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.blaze.plan.ArrowBroadcastExchangeBase
import org.apache.spark.sql.execution.blaze.plan.ArrowBroadcastExchangeExec

class BroadcastShimsImpl extends BroadcastShims {
  override def createArrowBroadcastExchange(
    mode: BroadcastMode,
    child: SparkPlan): ArrowBroadcastExchangeBase = ArrowBroadcastExchangeExec(mode, child)
}
