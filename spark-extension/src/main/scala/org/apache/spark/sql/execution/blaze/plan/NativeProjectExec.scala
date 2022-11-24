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

import java.util.UUID

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

import org.apache.spark.sql.blaze.MetricNode
import org.apache.spark.sql.blaze.NativeConverters
import org.apache.spark.sql.blaze.NativeRDD
import org.apache.spark.sql.blaze.NativeHelper
import org.apache.spark.sql.catalyst.analysis.ResolvedStar
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.execution.blaze.plan.NativeProjectExec.getNativeProjectBuilder
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.OneToOneDependency

import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.blaze.protobuf.PhysicalExprNode
import org.blaze.protobuf.PhysicalPlanNode
import org.blaze.protobuf.ProjectionExecNode

import org.apache.spark.sql.blaze.NativeSupports

case class NativeProjectExec(
    projectList: Seq[NamedExpression],
    override val child: SparkPlan,
    addTypeCast: Boolean = false)
    extends UnaryExecNode
    with NativeSupports {

  override lazy val metrics: Map[String, SQLMetric] = Map(
    NativeHelper
      .getDefaultNativeMetrics(sparkContext)
      .filterKeys(Set("output_rows", "elapsed_compute"))
      .toSeq: _*)

  override val output: Seq[Attribute] = projectList.map(_.toAttribute)
  override val outputPartitioning: Partitioning = child.outputPartitioning

  private val nativeProject = getNativeProjectBuilder(projectList, addTypeCast).buildPartial()

  override def doExecuteNative(): NativeRDD = {
    val inputRDD = NativeHelper.executeNative(child)
    val nativeMetrics = MetricNode(metrics, inputRDD.metrics :: Nil)

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      rddPartitions = inputRDD.partitions,
      rddDependencies = new OneToOneDependency(inputRDD) :: Nil,
      inputRDD.shuffleReadFull,
      (partition, taskContext) => {
        val inputPartition = inputRDD.partitions(partition.index)
        val nativeProjectExec = nativeProject.toBuilder
          .setInput(inputRDD.nativePlan(inputPartition, taskContext))
          .build()
        PhysicalPlanNode.newBuilder().setProjection(nativeProjectExec).build()
      },
      friendlyName = "NativeRDD.Project")
  }

  override def doCanonicalize(): SparkPlan = ProjectExec(projectList, child).canonicalized

  override def withNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
    copy(child = newChildren.head)
}

object NativeProjectExec {
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

      if (numAddedColumns == 0) {
        // add a dummy column when projection schema is empty because
        // native projection requires at least one column
        namedExprs.append(
          (
            "__dummy_" + UUID.randomUUID().toString,
            NativeConverters.convertExpr(Literal.apply(null, DataTypes.BooleanType))))
        numAddedColumns += 1
      }
      namedExprs
    }

    ProjectionExecNode
      .newBuilder()
      .addAllExprName(nativeNamedExprs.map(_._1).asJava)
      .addAllExpr(nativeNamedExprs.map(_._2).asJava)
  }
}
