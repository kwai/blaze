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

package org.apache.spark.sql.blaze

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.metric.SQLMetrics

case class ConvertToUnsafeRowExec(override val child: SparkPlan) extends UnaryExecNode {
  override def nodeName: String = "ConvertToUnsafeRow"
  override def logicalLink: Option[LogicalPlan] = child.logicalLink
  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = child.outputPartitioning

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numConvertedRows" -> SQLMetrics.createMetric(sparkContext, "number of converted rows"))

  override protected def doExecute(): RDD[InternalRow] = {
    val numConvertedRows = longMetric("numConvertedRows")
    val localOutput = this.output

    child.execute().mapPartitionsWithIndexInternal { (index, iterator) =>
      val toUnsafe = UnsafeProjection.create(localOutput, localOutput)
      toUnsafe.initialize(index)

      val convertedIterator = iterator.map {
        case row: UnsafeRow => row
        case row =>
          numConvertedRows += 1
          toUnsafe(row)
      }
      convertedIterator
    }
  }

  override def doCanonicalize(): SparkPlan = child.canonicalized
}
