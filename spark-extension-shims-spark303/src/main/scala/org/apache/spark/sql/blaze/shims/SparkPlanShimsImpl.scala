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

import java.lang.reflect.Method
import java.util.UUID

import org.apache.spark.SparkEnv
import org.apache.spark.SparkException
import org.blaze.protobuf.IpcReadMode
import org.blaze.protobuf.IpcReaderExecNode
import org.blaze.protobuf.PhysicalPlanNode
import org.blaze.protobuf.Schema

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.blaze.BlazeConvertStrategy.convertibleTag
import org.apache.spark.sql.blaze.{JniBridge, MetricNode, NativeRDD, NativeSupports, Shims, SparkPlanShims}
import org.apache.spark.sql.blaze.ForceNativeExecutionWrapper
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.CoalescedPartitionSpec
import org.apache.spark.sql.execution.PartialMapperPartitionSpec
import org.apache.spark.sql.execution.PartialReducerPartitionSpec
import org.apache.spark.sql.execution.ShufflePartitionSpec
import org.apache.spark.sql.execution.ShuffledRowRDD
import org.apache.spark.sql.execution.adaptive.{CustomShuffleReaderExec, QueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.blaze.plan.{ArrowShuffleExchangeExec, NativeParquetScanExec, NativeUnionExec}
import org.apache.spark.sql.execution.blaze.shuffle.ArrowBlockStoreShuffleReader
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec

private[blaze] class SparkPlanShimsImpl extends SparkPlanShims with Logging {
  override def isNative(plan: SparkPlan): Boolean =
    plan match {
      case _: NativeSupports => true
      case plan: CustomShuffleReaderExec => isNative(plan.child)
      case plan: QueryStageExec => isNative(plan.plan)
      case plan: ReusedExchangeExec => isNative(plan.child)
      case _ => false
    }

  override def getUnderlyingNativePlan(plan: SparkPlan): NativeSupports =
    plan match {
      case plan: NativeSupports => plan
      case plan: CustomShuffleReaderExec => getUnderlyingNativePlan(plan.child)
      case plan: QueryStageExec => getUnderlyingNativePlan(plan.plan)
      case plan: ReusedExchangeExec => getUnderlyingNativePlan(plan.child)
      case _ => throw new RuntimeException("unreachable: plan is not native")
    }

  override def executeNative(plan: SparkPlan): NativeRDD = {
    plan match {
      case plan: NativeSupports =>
        val executeQueryMethod: Method = classOf[SparkPlan]
          .getDeclaredMethod("executeQuery", classOf[() => _])
        executeQueryMethod.setAccessible(true)
        executeQueryMethod.invoke(plan, () => plan.doExecuteNative()).asInstanceOf[NativeRDD]

      case plan: CustomShuffleReaderExec => executeNativeCustomShuffleReader(plan)
      case plan: QueryStageExec => executeNative(plan.plan)
      case plan: ReusedExchangeExec => executeNative(plan.child)
      case _ => throw new SparkException(s"Underlying plan is not NativeSupports: ${plan}")
    }
  }

  override def isQueryStageInput(plan: SparkPlan): Boolean = {
    plan.isInstanceOf[QueryStageExec]
  }

  override def isShuffleQueryStageInput(plan: SparkPlan): Boolean = {
    plan.isInstanceOf[ShuffleQueryStageExec]
  }

  override def getChildStage(plan: SparkPlan): SparkPlan =
    plan.asInstanceOf[QueryStageExec].plan

  private def executeNativeCustomShuffleReader(exec: CustomShuffleReaderExec): NativeRDD = {
    exec match {
      case CustomShuffleReaderExec(child, _, _) if isNative(child) =>
        val inputShuffledRowRDD = exec.execute().asInstanceOf[ShuffledRowRDD]
        val shuffleHandle = inputShuffledRowRDD.dependency.shuffleHandle

        val inputRDD = executeNative(child)
        val nativeSchema: Schema = getUnderlyingNativePlan(child)
          .asInstanceOf[ArrowShuffleExchangeExec]
          .nativeSchema
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

  override def needRenameColumns(plan: SparkPlan): Boolean = {
    if (plan.output.isEmpty) {
      return false
    }
    // use shim to get isQueryStageInput and getChildStage
    plan match {
      case _: NativeParquetScanExec | _: NativeUnionExec | _: ReusedExchangeExec => true
      case exec: QueryStageExec =>
        needRenameColumns(Shims.get.sparkPlanShims.getChildStage(exec)) ||
          exec.output != Shims.get.sparkPlanShims.getChildStage(exec).output
      case exec: QueryStageExec => needRenameColumns(exec.plan)
      case CustomShuffleReaderExec(child, _, _) => needRenameColumns(child)
      case ForceNativeExecutionWrapper(child) => needRenameColumns(child)
      case _ => false
    }
  }

  override def simpleStringWithNodeId(plan: SparkPlan): String = plan.simpleStringWithNodeId()

  override def setLogicalLink(exec: SparkPlan, basedExec: SparkPlan): SparkPlan = {
    basedExec.logicalLink.foreach(logicalLink => exec.setLogicalLink(logicalLink))
    exec
  }
}
