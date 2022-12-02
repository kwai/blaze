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

package org.apache.spark.sql.blaze.shims

import java.util.UUID
import scala.annotation.tailrec
import org.apache.spark.OneToOneDependency
import org.apache.spark.ShuffleDependency
import org.apache.spark.SparkEnv
import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{RDD, ShuffledRDDPartition}
import org.apache.spark.shuffle.ShuffleReader
import org.apache.spark.sql.blaze.{
  JniBridge,
  MetricNode,
  NativeConverters,
  NativeRDD,
  NativeSupports,
  SparkPlanShims
}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{
  CoalescedPartitionSpec,
  PartialMapperPartitionSpec,
  PartialReducerPartitionSpec,
  ShufflePartitionSpec,
  ShuffledRowRDD
}
import org.apache.spark.sql.execution.adaptive.CustomShuffleReaderExec
import org.apache.spark.sql.types.{StructField, StructType}
import org.blaze.protobuf.{IpcReadMode, IpcReaderExecNode, PhysicalPlanNode, Schema}

import java.lang.reflect.Method
// import org.apache.spark.sql.blaze.kwai.BlazeOperatorMetricsCollector
import org.apache.spark.sql.execution.SparkPlan
// import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageInput
// import org.apache.spark.sql.execution.adaptive.LocalShuffledRowRDD
import org.apache.spark.sql.execution.adaptive.QueryStageExec
// import org.apache.spark.sql.execution.adaptive.ShuffleQueryStageInput
// import org.apache.spark.sql.execution.adaptive.SkewedShuffleQueryStageInput
import org.apache.spark.sql.execution.blaze.plan.ArrowShuffleExchangeExec
import org.apache.spark.sql.execution.blaze.shuffle.ArrowBlockStoreShuffleReader
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.metric.SQLShuffleReadMetricsReporter
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.Utils

private[blaze] class SparkPlanShimsImpl extends SparkPlan with SparkPlanShims with Logging {

  override def isNative(plan: SparkPlan): Boolean = {
    plan match {
      case _: NativeSupports => true
//      case plan: QueryStageInput => isNative(plan.childStage)
      case plan: QueryStageExec => isNative(plan.plan)
      case plan: ReusedExchangeExec => isNative(plan.child)
      case _ => false
    }
  }

  override def getUnderlyingNativePlan(plan: SparkPlan): NativeSupports = {
    plan match {
      case plan: NativeSupports => plan
      case plan: QueryStageExec => getUnderlyingNativePlan(plan.plan)
//      case plan: QueryStage => getUnderlyingNativePlan(plan.child)
      case plan: ReusedExchangeExec => getUnderlyingNativePlan(plan.child)
      case _ => throw new RuntimeException("unreachable: plan is not native")
    }
  }

  override def executeNative(plan: SparkPlan): NativeRDD = {
    plan match {
      case plan: SparkPlan with NativeSupports =>
        val executeQueryMethod: Method = classOf[SparkPlan]
          .getDeclaredMethod("executeQuery", classOf[() => _])
        executeQueryMethod.setAccessible(true)
        logInfo(s"plan name ${plan.nodeName} schema is: ${plan.schema}")
        val x: AnyRef = executeQueryMethod
          .invoke(plan, () => plan.doExecuteNative())
        logInfo(s"AnyRef is: $x")
        logInfo(s"invoke change ans is: ${x.asInstanceOf[NativeRDD]}")
        x.asInstanceOf[NativeRDD]
      case plan: CustomShuffleReaderExec => executeNativeCustomShuffleReader(plan, plan.output)
      case plan: QueryStageExec => executeNative(plan.plan)
      case plan: ReusedExchangeExec => executeNative(plan.child)
      case _ => throw new SparkException(s"Underlying plan is not NativeSupports: ${plan}")
    }
  }

  override def isQueryStageInput(plan: SparkPlan): Boolean = {
    plan.isInstanceOf[QueryStageExec]
//    plan.isInstanceOf[QueryStageInput]
  }

  override def getChildStage(plan: SparkPlan): SparkPlan =
    plan.asInstanceOf[QueryStageExec].plan

  private def executeNativeCustomShuffleReader(
      exec: CustomShuffleReaderExec,
      output: Seq[Attribute]): NativeRDD = {
    exec match {
      case CustomShuffleReaderExec(_, _, _) =>
        val inputShuffledRowRDD = exec.execute().asInstanceOf[ShuffledRowRDD]
        val shuffleHandle = inputShuffledRowRDD.dependency.shuffleHandle

        val inputRDD = executeNative(exec.child)
        val nativeSchema: Schema = NativeConverters.convertSchema(StructType(output.map(a =>
          StructField(a.toString(), a.dataType, a.nullable, a.metadata))))
        val metrics = MetricNode(Map(), inputRDD.metrics :: Nil)

        new NativeRDD(
          inputShuffledRowRDD.sparkContext,
          metrics,
          inputShuffledRowRDD.partitions,
          inputShuffledRowRDD.dependencies,
          true,
          (partition, taskContext) => {

            // use reflection to get partitionSpec because ShuffledRowRDDPartition is private
            // scalastyle:off classforname
            val shuffledRDDPartitionClass =
              Class.forName("org.apache.spark.sql.execution.ShuffledRowRDDPartition")
            // scalastyle:on classforname
            val specField = shuffledRDDPartitionClass.getDeclaredField("spec")
            specField.setAccessible(true)
            val sqlMetricsReporter = taskContext.taskMetrics().createTempShuffleReadMetrics()
            val spec = specField.get(partition).asInstanceOf[ShufflePartitionSpec]
            val reader = spec match {
              case CoalescedPartitionSpec(startReducerIndex, endReducerIndex) =>
                SparkEnv.get.shuffleManager
                  .getReader(
                    shuffleHandle,
                    startReducerIndex,
                    endReducerIndex,
                    taskContext,
                    sqlMetricsReporter)
                  .asInstanceOf[ArrowBlockStoreShuffleReader[_, _]]

              case PartialReducerPartitionSpec(reducerIndex, startMapIndex, endMapIndex) =>
                SparkEnv.get.shuffleManager
                  .getReaderForRange(
                    shuffleHandle,
                    startMapIndex,
                    endMapIndex,
                    reducerIndex,
                    reducerIndex + 1,
                    taskContext,
                    sqlMetricsReporter)
                  .asInstanceOf[ArrowBlockStoreShuffleReader[_, _]]

              case PartialMapperPartitionSpec(mapIndex, startReducerIndex, endReducerIndex) =>
                SparkEnv.get.shuffleManager
                  .getReaderForRange(
                    shuffleHandle,
                    mapIndex,
                    mapIndex + 1,
                    startReducerIndex,
                    endReducerIndex,
                    taskContext,
                    sqlMetricsReporter)
                  .asInstanceOf[ArrowBlockStoreShuffleReader[_, _]]
            }

            // store fetch iterator in jni resource before native compute
            val jniResourceId = s"NativeShuffleReadExec:${UUID.randomUUID().toString}"
            JniBridge.resourcesMap.put(jniResourceId, () => reader.readIpc())

            PhysicalPlanNode
              .newBuilder()
              .setIpcReader(
                IpcReaderExecNode
                  .newBuilder()
                  .setSchema(nativeSchema)
                  .setNumPartitions(inputShuffledRowRDD.getNumPartitions)
                  .setIpcProviderResourceId(jniResourceId)
                  .setMode(IpcReadMode.CHANNEL_AND_FILE_SEGMENT)
                  .build())
              .build()
          })
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException("SparkPlanShimsImpl does not support doExecute()")
  }

  override def output: Seq[Attribute] = {
    throw new UnsupportedOperationException("SparkPlanShimsImpl does not support output()")
  }

  override def children: Seq[SparkPlan] = {
    throw new UnsupportedOperationException("SparkPlanShimsImpl does not support children()")
  }

  override def productElement(n: Int): Any = {
    throw new UnsupportedOperationException(
      "SparkPlanShimsImpl does not support productElement()")
  }

  override def productArity: Int = {
    throw new UnsupportedOperationException("SparkPlanShimsImpl does not support productArity()")
  }

  override def canEqual(that: Any): Boolean = {
    throw new UnsupportedOperationException("SparkPlanShimsImpl does not support canEqual()")
  }
}
