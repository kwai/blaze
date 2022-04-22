package org.apache.spark.sql.blaze.plan

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.blaze.MetricNode
import org.apache.spark.sql.blaze.NativeConverters
import org.apache.spark.sql.blaze.NativeRDD
import org.apache.spark.sql.blaze.NativeSupports
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.metric.SQLMetric
import org.blaze.protobuf.FilterExecNode
import org.blaze.protobuf.PhysicalPlanNode

case class NativeFilterExec(condition: Expression, override val child: SparkPlan)
    extends UnaryExecNode
    with NativeSupports {

  override lazy val metrics: Map[String, SQLMetric] =
    NativeSupports.getDefaultNativeMetrics(sparkContext)

  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  private val nativeFilterExpr = NativeConverters.convertExpr(condition)

  override def doExecute(): RDD[InternalRow] = doExecuteNative()
  override def doExecuteNative(): NativeRDD = {
    val inputRDD = NativeSupports.executeNative(child)
    val nativeMetrics = MetricNode(
      Map(
        "output_rows" -> metrics("numOutputRows"),
        "blaze_output_ipc_rows" -> metrics("blazeExecIPCWrittenRows"),
        "blaze_output_ipc_bytes" -> metrics("blazeExecIPCWrittenBytes"),
        "blaze_exec_time" -> metrics("blazeExecTime")),
      Seq(inputRDD.metrics))

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      inputRDD.partitions,
      inputRDD.dependencies,
      (partition, taskContext) => {
        val nativeFilterExec = FilterExecNode
          .newBuilder()
          .setInput(inputRDD.nativePlan(partition, taskContext))
          .setExpr(nativeFilterExpr)
          .build()
        PhysicalPlanNode.newBuilder().setFilter(nativeFilterExec).build()
      })
  }
}
