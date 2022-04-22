package org.apache.spark.sql.blaze.plan

import org.apache.spark.internal.config.RDD_PARALLEL_LISTING_THRESHOLD
import org.apache.spark.{Dependency, Partition, RangeDependency}
import org.apache.spark.rdd.{RDD, UnionPartition, UnionRDD}
import org.apache.spark.sql.blaze.MetricNode
import org.apache.spark.sql.blaze.NativeRDD
import org.apache.spark.sql.blaze.NativeSupports
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType
import org.blaze.protobuf.{PhysicalPlanNode, UnionExecNode}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

case class NativeUnionExec(override val children: Seq[SparkPlan])
    extends SparkPlan
    with NativeSupports {

  override lazy val metrics: Map[String, SQLMetric] =
    NativeSupports.getDefaultNativeMetrics(sparkContext)

  // updating nullability to make all the children consistent
  override def output: Seq[Attribute] = {
    children.map(_.output).transpose.map { attrs =>
      val firstAttr = attrs.head
      val nullable = attrs.exists(_.nullable)
      val newDt = attrs.map(_.dataType).reduce(StructType.merge)
      if (firstAttr.dataType == newDt) {
        firstAttr.withNullability(nullable)
      } else {
        AttributeReference(firstAttr.name, newDt, nullable, firstAttr.metadata)(
          firstAttr.exprId,
          firstAttr.qualifier)
      }
    }
  }

  override def doExecute(): RDD[InternalRow] = doExecuteNative()

  override def doExecuteNative(): NativeRDD = {
    val rdds = children.map(c => NativeSupports.executeNative(c))
    val nativeMetrics = MetricNode(
      Map(
        "output_rows" -> metrics("numOutputRows"),
        "blaze_output_ipc_rows" -> metrics("blazeExecIPCWrittenRows"),
        "blaze_output_ipc_bytes" -> metrics("blazeExecIPCWrittenBytes"),
        "blaze_exec_time" -> metrics("blazeExecTime")),
      rdds.map(r => r.metrics))

    def partitions: Array[Partition] = {
      val array = new Array[Partition](rdds.map(_.partitions.length).sum)
      var pos = 0
      for ((rdd, rddIndex) <- rdds.zipWithIndex; split <- rdd.partitions) {
        array(pos) = new UnionPartition(pos, rdd, rddIndex, split.index)
        pos += 1
      }
      array
    }

    def dependencies: Seq[Dependency[_]] = {
      val deps = new ArrayBuffer[Dependency[_]]
      var pos = 0
      for (rdd <- rdds) {
        deps += new RangeDependency(rdd, 0, pos, rdd.partitions.length)
        pos += rdd.partitions.length
      }
      deps
    }

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      partitions,
      dependencies,
      (partition, taskContext) => {
        val inputs = rdds.map(r => r.nativePlan(partition, taskContext))
        val union = UnionExecNode.newBuilder().addAllChildren(inputs.asJava)
        PhysicalPlanNode.newBuilder().setUnion(union).build()
      })
  }
}
