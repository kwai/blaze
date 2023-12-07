package org.apache.spark.sql

import org.blaze.protobuf.{PhysicalExprNode, PhysicalPlanNode}

import java.util.Base64

object ProtoTextConverters {

  def convertExpr(expr: PhysicalExprNode): String = {
    //    java.util.Base64.getEncoder.encodeToString(computingNativePlan.toByteArray)
    Base64.getEncoder.encodeToString(expr.toByteArray)
  }

  def convertPlan(plan: PhysicalPlanNode): String = {
    Base64.getEncoder.encodeToString(plan.toByteArray)
  }

}
