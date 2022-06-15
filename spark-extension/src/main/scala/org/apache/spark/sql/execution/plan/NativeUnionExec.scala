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

package org.apache.spark.sql.execution.plan

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

import org.apache.spark.Dependency
import org.apache.spark.Partition
import org.apache.spark.RangeDependency
import org.apache.spark.rdd.UnionPartition
import org.apache.spark.sql.blaze.MetricNode
import org.apache.spark.sql.blaze.NativeConverters
import org.apache.spark.sql.blaze.NativeRDD
import org.apache.spark.sql.blaze.NativeSupports
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.UnionExec
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.blaze.protobuf.EmptyPartitionsExecNode
import org.blaze.protobuf.PhysicalPlanNode
import org.blaze.protobuf.Schema
import org.blaze.protobuf.UnionExecNode

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

  override def doExecuteNative(): NativeRDD = {
    val rdds = children.map(c => NativeSupports.executeNative(c))
    val nativeMetrics = MetricNode(metrics, rdds.map(_.metrics))

    def unionedPartitions: Array[UnionPartition[InternalRow]] = {
      val array = new Array[UnionPartition[InternalRow]](rdds.map(_.partitions.length).sum)
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
      unionedPartitions.asInstanceOf[Array[Partition]],
      dependencies,
      (partition, taskContext) => {
        val unionPartition = unionedPartitions(partition.index)
        val unionChildrenExecs = rdds.zipWithIndex.map {
          case (rdd, rddIndex) if rddIndex == unionPartition.parentRddIndex =>
            rdd.nativePlan(unionPartition.parentPartition, taskContext)
          case (rdd, _) =>
            nativeEmptyPartitionExec(rdd.getNumPartitions)
        }
        val union = UnionExecNode.newBuilder().addAllChildren(unionChildrenExecs.asJava)
        PhysicalPlanNode.newBuilder().setUnion(union).build()
      })
  }

  private def nativeEmptyPartitionExec(numPartitions: Int) =
    PhysicalPlanNode
      .newBuilder()
      .setEmptyPartitions(
        EmptyPartitionsExecNode
          .newBuilder()
          .setSchema(nativeSchema)
          .setNumPartitions(numPartitions)
          .build())
      .build()

  val nativeSchema: Schema = NativeConverters.convertSchema(
    StructType(output.map(a => StructField(a.toString(), a.dataType, a.nullable, a.metadata))))

  override def doCanonicalize(): SparkPlan = UnionExec(children).canonicalized
}
