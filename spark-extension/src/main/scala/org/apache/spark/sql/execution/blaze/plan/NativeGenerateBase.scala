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
import scala.collection.immutable.SortedMap
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.OneToOneDependency
import org.apache.spark.sql.blaze.MetricNode
import org.apache.spark.sql.blaze.NativeConverters
import org.apache.spark.sql.blaze.NativeHelper
import org.apache.spark.sql.blaze.NativeRDD
import org.apache.spark.sql.blaze.NativeSupports
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.catalyst.expressions.Explode
import org.apache.spark.sql.catalyst.expressions.Generator
import org.apache.spark.sql.catalyst.expressions.JsonTuple
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.PosExplode
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.blaze.{protobuf => pb}
import org.blaze.protobuf.PhysicalPlanNode

import com.google.protobuf.ByteString

abstract class NativeGenerateBase(
    generator: Generator,
    requiredChildOutput: Seq[Attribute],
    outer: Boolean,
    generatorOutput: Seq[Attribute],
    override val child: SparkPlan)
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

  override def output: Seq[Attribute] = requiredChildOutput ++ generatorOutput
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = Nil

  private def nativeGenerator = generator match {
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
    case JsonTuple(children) if children.drop(1).forall(_.isInstanceOf[Literal]) =>
      pb.Generator
        .newBuilder()
        .setFunc(pb.GenerateFunction.JsonTuple)
        .addAllChild(children.map(NativeConverters.convertExpr).asJava)
        .build()
    case udtf =>
      val children = ArrayBuffer[AttributeReference]()
      val paramsFields = ArrayBuffer[StructField]()

      val bound = udtf.mapChildren(_.transformDown {
        case p: AttributeReference =>
          children += p
          paramsFields += StructField(p.name, p.dataType, p.nullable)
          BoundReference(paramsFields.length - 1, p.dataType, p.nullable)
        case p => p
      })
      pb.Generator
        .newBuilder()
        .setFunc(pb.GenerateFunction.Udtf)
        .setUdtf(
          pb.GenerateUdtf
            .newBuilder()
            .setSerialized(ByteString.copyFrom(NativeConverters.serializeExpression(
              bound.asInstanceOf[Generator with Serializable],
              StructType(paramsFields))))
            .setReturnSchema(NativeConverters.convertSchema(udtf.elementSchema)))
        .addAllChild(children.map(NativeConverters.convertExpr).asJava)
        .build()
  }

  private def nativeGeneratorOutput =
    Util.getSchema(generatorOutput).map(NativeConverters.convertField)

  private def nativeRequiredChildOutput =
    Util.getSchema(requiredChildOutput).map(_.name)

  // check whether native converting is supported
  nativeGenerator
  nativeGeneratorOutput
  nativeRequiredChildOutput

  override def doExecuteNative(): NativeRDD = {
    val inputRDD = NativeHelper.executeNative(child)
    val nativeMetrics = MetricNode(metrics, inputRDD.metrics :: Nil)
    val nativeGenerator = this.nativeGenerator
    val nativeGeneratorOutput = this.nativeGeneratorOutput
    val nativeRequiredChildOutput = this.nativeRequiredChildOutput

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
}
