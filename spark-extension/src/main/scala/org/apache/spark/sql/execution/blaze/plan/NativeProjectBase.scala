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
import org.apache.spark.sql.catalyst.analysis.ResolvedStar
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.blaze.plan.NativeProjectBase.getNativeProjectBuilder
import org.apache.spark.OneToOneDependency
import org.blaze.protobuf.PhysicalExprNode
import org.blaze.protobuf.PhysicalPlanNode
import org.blaze.protobuf.ProjectionExecNode
import org.apache.spark.sql.blaze.NativeSupports
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.physical.Partitioning

abstract class NativeProjectBase(
    projectList: Seq[NamedExpression],
    override val child: SparkPlan,
    addTypeCast: Boolean = false)
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

  override def output: Seq[Attribute] = projectList.map(_.toAttribute)
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  private def nativeProject = getNativeProjectBuilder(projectList, addTypeCast).buildPartial()

  // check whether native converting is supported
  nativeProject

  override def doExecuteNative(): NativeRDD = {
    val inputRDD = NativeHelper.executeNative(child)
    val nativeMetrics = MetricNode(metrics, inputRDD.metrics :: Nil)
    val nativeProject = this.nativeProject

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      rddPartitions = inputRDD.partitions,
      rddDependencies = new OneToOneDependency(inputRDD) :: Nil,
      inputRDD.isShuffleReadFull,
      (partition, taskContext) => {
        val inputPartition = inputRDD.partitions(partition.index)
        val nativeProjectExec = nativeProject.toBuilder
          .setInput(inputRDD.nativePlan(inputPartition, taskContext))
          .build()
        PhysicalPlanNode.newBuilder().setProjection(nativeProjectExec).build()
      },
      friendlyName = "NativeRDD.Project")
  }
}

object NativeProjectBase {
  def getNativeProjectBuilder(
      projectList: Seq[NamedExpression],
      addTypeCast: Boolean = false): ProjectionExecNode.Builder = {
    val nativeNamedExprs: Seq[(String, PhysicalExprNode)] = {
      val namedExprs = ArrayBuffer[(String, PhysicalExprNode)]()
      var numAddedColumns = 0

      val castedProjectList = if (!addTypeCast) {
        projectList
      } else {
        projectList.map { projectExpr =>
          projectExpr
            .mapChildren(child => Cast(child, child.dataType))
            .asInstanceOf[NamedExpression]
        }
      }

      castedProjectList.foreach { projectExpr =>
        def addNamedExpression(namedExpression: NamedExpression): Unit = {
          namedExpression match {
            case star: ResolvedStar =>
              for (expr <- star.expressions) {
                addNamedExpression(expr)
              }

            case alias: Alias =>
              namedExprs.append(
                (Util.getFieldNameByExprId(alias), NativeConverters.convertExpr(alias.child)))
              numAddedColumns += 1

            case named =>
              namedExprs.append(
                (Util.getFieldNameByExprId(named), NativeConverters.convertExpr(named)))
              numAddedColumns += 1
          }
        }
        addNamedExpression(projectExpr)
      }
      namedExprs
    }

    ProjectionExecNode
      .newBuilder()
      .addAllExprName(nativeNamedExprs.map(_._1).asJava)
      .addAllExpr(nativeNamedExprs.map(_._2).asJava)
  }
}
