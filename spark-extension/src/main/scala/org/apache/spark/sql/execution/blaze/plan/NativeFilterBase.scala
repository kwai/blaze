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
package org.apache.spark.sql.execution.blaze.plan

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import scala.collection.immutable.SortedMap

import org.apache.spark.sql.blaze.MetricNode
import org.apache.spark.sql.blaze.NativeConverters
import org.apache.spark.sql.blaze.NativeRDD
import org.apache.spark.sql.blaze.NativeHelper
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.OneToOneDependency
import org.blaze.protobuf.FilterExecNode
import org.blaze.protobuf.PhysicalExprNode
import org.blaze.protobuf.PhysicalPlanNode
import org.apache.spark.sql.blaze.NativeSupports
import org.apache.spark.sql.catalyst.expressions.And
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.IsNotNull
import org.apache.spark.sql.execution.FilterExec

abstract class NativeFilterBase(condition: Expression, override val child: SparkPlan)
    extends UnaryExecNode
    with NativeSupports {

  override lazy val metrics: Map[String, SQLMetric] = SortedMap[String, SQLMetric]() ++ Map(
    NativeHelper
      .getDefaultNativeMetrics(sparkContext)
      .filterKeys(
        Set(
          "stage_id",
          "output_rows",
          "elapsed_compute",
          "input_batch_count",
          "input_batch_mem_size",
          "input_row_count"))
      .toSeq: _*)

  override def output: Seq[Attribute] = FilterExec(condition, child).output
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  private def nativeFilterExprs = {
    val splittedExprs = ArrayBuffer[PhysicalExprNode]()

    // do not split simple IsNotNull(col) exprs
    def isNaiveIsNotNullColumns(expr: Expression): Boolean = {
      expr match {
        case IsNotNull(_: AttributeReference) => true
        case And(lhs, rhs) if isNaiveIsNotNullColumns(lhs) && isNaiveIsNotNullColumns(rhs) => true
        case _ => false
      }
    }
    def split(expr: Expression): Unit = {
      expr match {
        case e @ And(lhs, rhs) if !isNaiveIsNotNullColumns(e) =>
          split(lhs)
          split(rhs)
        case expr => splittedExprs.append(NativeConverters.convertExpr(expr))
      }
    }
    split(condition)
    splittedExprs
  }

  // check whether native converting is supported
  nativeFilterExprs

  override def doExecuteNative(): NativeRDD = {
    val inputRDD = NativeHelper.executeNative(child)
    val nativeMetrics = MetricNode(metrics, inputRDD.metrics :: Nil)
    val nativeFilterExprs = this.nativeFilterExprs
    new NativeRDD(
      sparkContext,
      nativeMetrics,
      rddPartitions = inputRDD.partitions,
      rddDependencies = new OneToOneDependency(inputRDD) :: Nil,
      inputRDD.isShuffleReadFull,
      (partition, taskContext) => {
        val inputPartition = inputRDD.partitions(partition.index)
        val nativeFilterExec = FilterExecNode
          .newBuilder()
          .setInput(inputRDD.nativePlan(inputPartition, taskContext))
          .addAllExpr(nativeFilterExprs.asJava)
          .build()
        PhysicalPlanNode.newBuilder().setFilter(nativeFilterExec).build()
      },
      friendlyName = "NativeRDD.Filter")
  }
}
