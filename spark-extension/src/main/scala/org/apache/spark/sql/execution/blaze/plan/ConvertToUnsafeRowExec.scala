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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.CodegenSupport
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.UnaryExecNode

case class ConvertToUnsafeRowExec(override val child: SparkPlan)
    extends UnaryExecNode
    with CodegenSupport {
  override def nodeName: String = "ConvertToUnsafeRow"

  override def logicalLink: Option[LogicalPlan] = child.logicalLink

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  private lazy val inputRDD = child.execute()

  override protected def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val localOutput = this.output

    inputRDD.mapPartitionsWithIndexInternal { (index, iterator) =>
      val toUnsafe = UnsafeProjection.create(localOutput, localOutput)
      toUnsafe.initialize(index)

      val convertedIterator = iterator.map {
        case row: UnsafeRow => row
        case row =>
          numOutputRows += 1
          toUnsafe(row)
      }
      convertedIterator
    }
  }

  override def doProduce(ctx: CodegenContext): String = {
    val input = ctx.addMutableState("scala.collection.Iterator", "input", v => s"$v = inputs[0];")
    val row = ctx.freshName("row")
    val numOutputRows = metricTerm(ctx, "numOutputRows")

    val outputVars = {
      ctx.INPUT_ROW = row
      ctx.currentVars = null
      this.output.zipWithIndex.map {
        case (a, i) =>
          BoundReference(i, a.dataType, a.nullable).genCode(ctx)
      }
    }

    s"""
       | while ($limitNotReachedCond $input.hasNext()) {
       |   InternalRow $row = (InternalRow) $input.next();
       |   ${consume(ctx, outputVars).trim}
       |   $numOutputRows.add(1);
       |   $shouldStopCheckCode
       | }
     """.stripMargin
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = inputRDD :: Nil

  override def doCanonicalize(): SparkPlan = child.canonicalized
}
