package org.apache.spark.sql.blaze.plan

import scala.collection.JavaConverters._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.blaze.MetricNode
import org.apache.spark.sql.blaze.NativeConverters
import org.apache.spark.sql.blaze.NativeRDD
import org.apache.spark.sql.blaze.NativeSupports
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.expressions.NullsFirst
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.physical.Distribution
import org.apache.spark.sql.catalyst.plans.physical.OrderedDistribution
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.plans.physical.UnspecifiedDistribution
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.metric.SQLMetric
import org.blaze.protobuf.PhysicalExprNode
import org.blaze.protobuf.PhysicalPlanNode
import org.blaze.protobuf.PhysicalSortExprNode
import org.blaze.protobuf.SortExecNode

case class NativeSortExec(
    sortOrder: Seq[SortOrder],
    global: Boolean,
    override val child: SparkPlan)
    extends UnaryExecNode
    with NativeSupports {

  override lazy val metrics: Map[String, SQLMetric] =
    NativeSupports.getDefaultNativeMetrics(sparkContext)

  override def output: Seq[Attribute] = child.output

  override def outputOrdering: Seq[SortOrder] = sortOrder

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] =
    if (global) {
      OrderedDistribution(sortOrder) :: Nil
    } else {
      UnspecifiedDistribution :: Nil
    }

  private val nativeSortExprs = sortOrder.map { sortOrder =>
    PhysicalExprNode
      .newBuilder()
      .setSort(
        PhysicalSortExprNode
          .newBuilder()
          .setExpr(NativeConverters.convertExpr(sortOrder.child))
          .setAsc(sortOrder.direction == Ascending)
          .setNullsFirst(sortOrder.nullOrdering == NullsFirst)
          .build())
      .build()
  }

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
        val nativeSortExec = SortExecNode
          .newBuilder()
          .setInput(inputRDD.nativePlan(partition, taskContext))
          .addAllExpr(nativeSortExprs.asJava)
          .build()
        PhysicalPlanNode.newBuilder().setSort(nativeSortExec).build()
      })
  }
}
