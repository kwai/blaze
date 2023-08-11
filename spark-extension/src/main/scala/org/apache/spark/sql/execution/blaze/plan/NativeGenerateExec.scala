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

import org.apache.spark.OneToOneDependency
import org.apache.spark.sql.blaze.MetricNode
import org.apache.spark.sql.blaze.NativeConverters
import org.apache.spark.sql.blaze.NativeHelper
import org.apache.spark.sql.blaze.NativeRDD
import org.apache.spark.sql.blaze.NativeSupports
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Explode
import org.apache.spark.sql.catalyst.expressions.Generator
import org.apache.spark.sql.catalyst.expressions.PosExplode
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.GenerateExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.metric.SQLMetric
import org.blaze.{protobuf => pb}
import org.blaze.protobuf.PhysicalPlanNode

case class NativeGenerateExec(
    generator: Generator,
    requiredChildOutput: Seq[Attribute],
    outer: Boolean,
    generatorOutput: Seq[Attribute],
    override val child: SparkPlan)
    extends UnaryExecNode
    with NativeSupports {

  override lazy val metrics: Map[String, SQLMetric] = Map(
    NativeHelper
      .getDefaultNativeMetrics(sparkContext)
      .filterKeys(Set("output_rows", "elapsed_compute"))
      .toSeq: _*)

  override def output: Seq[Attribute] = requiredChildOutput ++ generatorOutput
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = Nil

  private val nativeGenerator = generator match {
    case Explode(child) =>
      pb.Generator
        .newBuilder()
        .setFunc(pb.GenerateFunction.Explode)
        .addChild(NativeConverters.convertExpr(child))
        .build()
    case PosExplode(child) =>
      pb.Generator
        .newBuilder()
        .setFunc(pb.GenerateFunction.PosExplode)
        .addChild(NativeConverters.convertExpr(child))
        .build()
    case other =>
      throw new NotImplementedError(s"generator not supported: $other")
  }

  private val nativeGeneratorOutput =
    Util.getSchema(generatorOutput).map(NativeConverters.convertField)

  private val nativeRequiredChildOutput =
    Util.getSchema(requiredChildOutput).map(_.name)

  override def doExecuteNative(): NativeRDD = {
    val inputRDD = NativeHelper.executeNative(child)
    val nativeMetrics = MetricNode(metrics, inputRDD.metrics :: Nil)

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      rddPartitions = inputRDD.partitions,
      rddDependencies = new OneToOneDependency(inputRDD) :: Nil,
      inputRDD.isShuffleReadFull,
      (partition, taskContext) => {
        val inputPartition = inputRDD.partitions(partition.index)
        val nativeGenerateExec = pb.GenerateExecNode
          .newBuilder()
          .setInput(inputRDD.nativePlan(inputPartition, taskContext))
          .setGenerator(nativeGenerator)
          .addAllGeneratorOutput(nativeGeneratorOutput.asJava)
          .addAllRequiredChildOutput(nativeRequiredChildOutput.asJava)
          .setOuter(outer)
          .build()
        PhysicalPlanNode.newBuilder().setGenerate(nativeGenerateExec).build()
      },
      friendlyName = "NativeRDD.Generate")
  }

  override def doCanonicalize(): SparkPlan =
    GenerateExec(generator, requiredChildOutput, outer, generatorOutput, child).canonicalized

  override def withNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
    copy(child = newChildren.head)
}
