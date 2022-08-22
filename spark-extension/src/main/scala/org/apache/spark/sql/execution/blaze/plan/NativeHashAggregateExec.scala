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

import scala.collection.JavaConverters._

import org.apache.spark.sql.blaze.MetricNode
import org.apache.spark.sql.blaze.NativeConverters
import org.apache.spark.sql.blaze.NativeRDD
import org.apache.spark.sql.blaze.NativeSupports
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.AttributeSeq
import org.apache.spark.sql.catalyst.expressions.aggregate
import org.apache.spark.sql.catalyst.expressions.aggregate.Complete
import org.apache.spark.sql.catalyst.expressions.aggregate.Final
import org.apache.spark.sql.catalyst.expressions.aggregate.Partial
import org.apache.spark.sql.catalyst.expressions.aggregate.PartialMerge
import org.apache.spark.sql.catalyst.plans.physical.AllTuples
import org.apache.spark.sql.catalyst.plans.physical.ClusteredDistribution
import org.apache.spark.sql.catalyst.plans.physical.Distribution
import org.apache.spark.sql.catalyst.plans.physical.UnspecifiedDistribution
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.blaze.protobuf.AggregateMode
import org.blaze.protobuf.HashAggregateExecNode
import org.blaze.protobuf.PhysicalExprNode
import org.blaze.protobuf.PhysicalPlanNode
import org.blaze.protobuf.Schema

case class NativeHashAggregateExec(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    resultExpressions: Seq[NamedExpression],
    override val child: SparkPlan)
    extends UnaryExecNode
    with NativeSupports {

  val aggrMode: aggregate.AggregateMode = aggregateExpressions.head.mode

  override def output: Seq[Attribute] =
    aggrMode match {
      case Partial =>
        groupingExpressions.map(_.toAttribute) ++ aggregateAttributes
      case _ =>
        resultExpressions.map(_.toAttribute)
    }

  override def requiredChildDistribution: List[Distribution] = {
    requiredChildDistributionExpressions match {
      case Some(exprs) if exprs.isEmpty => AllTuples :: Nil
      case Some(exprs) => ClusteredDistribution(exprs) :: Nil
      case None => UnspecifiedDistribution :: Nil
    }
  }

  override lazy val metrics: Map[String, SQLMetric] =
    NativeSupports.getDefaultNativeMetrics(sparkContext)

  override lazy val allAttributes: AttributeSeq =
    child.output ++ aggregateBufferAttributes ++ aggregateAttributes ++
      aggregateExpressions.flatMap(_.aggregateFunction.inputAggBufferAttributes)

  protected val aggregateBufferAttributes: Seq[AttributeReference] = {
    aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)
  }

  override def doExecuteNative(): NativeRDD = {
    val inputRDD = NativeSupports.executeNative(child)
    val partialNativeMetrics = MetricNode(metrics, inputRDD.metrics :: Nil)
    val finalNativeMetrics = {
      val renameColumnsMetrics = MetricNode(Map(), inputRDD.metrics :: Nil)
      MetricNode(metrics, renameColumnsMetrics :: Nil)
    }

    val nativeMetrics = if (aggrMode == Partial) {
      partialNativeMetrics
    } else {
      finalNativeMetrics
    }

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      inputRDD.partitions,
      inputRDD.dependencies,
      inputRDD.shuffleReadFull,
      (partition, taskContext) => {

        lazy val partialInputPlan =
          inputRDD.nativePlan(inputRDD.partitions(partition.index), taskContext)

        lazy val finalInputPlan =
          NativeRenameColumnsExec.buildRenameColumnsExec(
            partialInputPlan,
            child.output.map(_.toString))

        def buildAggrPlan(buildFn: HashAggregateExecNode.Builder => Unit) = {
          val builder = HashAggregateExecNode
            .newBuilder()
            .addAllAggrExprName(aggregateAttributes.map(_.toString).asJava)
            .addAllGroupExpr(nativeGroupingExprs.asJava)
            .addAllGroupExprName(groupingExpressions.map(_.toAttribute.toString()).asJava)
            .setInputSchema(nativeInputSchema)
          buildFn(builder)
          PhysicalPlanNode.newBuilder().setHashAggregate(builder).build()
        }

        aggrMode match {
          case Partial =>
            buildAggrPlan(
              _.setMode(AggregateMode.PARTIAL)
                .setInput(partialInputPlan)
                .addAllAggrExpr(nativePartialAggrExprs.asJava))

          case PartialMerge | Final =>
            buildAggrPlan(
              _.setMode(AggregateMode.FINAL)
                .setInput(finalInputPlan)
                .addAllAggrExpr(nativeFinalAggrExprs.asJava))

          case Complete =>
            val partialAggrPlan = buildAggrPlan(
              _.setMode(AggregateMode.PARTIAL)
                .setInput(partialInputPlan)
                .addAllAggrExpr(nativePartialAggrExprs.asJava))
            buildAggrPlan(
              _.setInput(partialAggrPlan)
                .setMode(AggregateMode.FINAL)
                .addAllAggrExpr(nativeFinalAggrExprs.asJava))
        }
      },
      friendlyName = "NativeRDD.HashAggregate")
  }

  val nativeInputSchema: Schema = NativeConverters.convertSchema(StructType(child.output.map(a =>
    StructField(a.toString(), a.dataType, a.nullable, a.metadata))))

  val nativeGroupingExprs: Seq[PhysicalExprNode] = groupingExpressions.map { expr =>
    NativeConverters.convertExpr(expr)
  }

  val nativePartialAggrExprs: Seq[PhysicalExprNode] = aggregateExpressions.map { expr =>
    assert(!expr.isDistinct)
    NativeConverters.convertExpr(expr.aggregateFunction)
  }

  val nativeFinalAggrExprs: Seq[PhysicalExprNode] = aggrMode match {
    case Partial => null
    case _ =>
      aggregateExpressions.zipWithIndex.map {
        case (aggrExpr, index) =>
          val attr = child.output(groupingExpressions.length + index)
          NativeConverters.convertExpr(aggrExpr.aggregateFunction.mapChildren(_ => attr))
      }
  }

  override def doCanonicalize(): SparkPlan =
    HashAggregateExec(
      requiredChildDistributionExpressions,
      groupingExpressions,
      aggregateExpressions,
      aggregateAttributes,
      initialInputBufferOffset = 0,
      resultExpressions,
      child).canonicalized

  override val nodeName: String =
    s"NativeHashAggregate.${aggrMode.getClass.getSimpleName}"

  //override def simpleString(maxFields: Int): String = {
  //  if (aggrMode == Partial) {
  //    s"$nodeName grouping=[${groupingExpressions.mkString(", ")}], aggr=[${aggregateExpressions.mkString(", ")}]"
  //  } else {
  //    s"$nodeName output=[${resultExpressions.map(_.toAttribute.toString).mkString(", ")}]"
  //  }
  //}

  //override def simpleStringWithNodeId(): String = super.simpleStringWithNodeId()
}
