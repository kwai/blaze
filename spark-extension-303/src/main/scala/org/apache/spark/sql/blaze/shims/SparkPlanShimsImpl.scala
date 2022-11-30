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
import org.apache.spark.sql.blaze.{JniBridge, NativeRDD, NativeSupports, SparkPlanShims}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.adaptive.{
  BroadcastQueryStageExec,
  CustomShuffleReaderExec,
  ShuffleQueryStageExec
}
import org.blaze.protobuf.{IpcReadMode, IpcReaderExecNode, PhysicalPlanNode}
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

private[blaze] case class SparkPlanShimsImpl()
    extends SparkPlanShims
    with SparkPlan
    with Logging {

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
      case plan: NativeSupports =>
        plan.executeQuery {
          plan.doExecuteNative()
        }
      case plan: ShuffleQueryStageExec => executeNativeCustomShuffleReader(plan.shuffle)
      case plan: CustomShuffleReaderExec => executeNativeCustomShuffleReader(plan)
      case plan: BroadcastQueryStageExec => executeNative(plan.broadcast)
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

  private def executeNativeCustomShuffleReader(exec: SparkPlan): NativeRDD = {
    exec match {
//      case _: ShuffleQueryStageExec | _: SkewedShuffleQueryStageInput =>
      case _: ShuffleQueryStageExec | _: CustomShuffleReaderExec =>
        val shuffledRDD = exec.execute()
        val dependency = shuffledRDD.getClass
          .getMethod("dependency")
          .invoke(shuffledRDD)
          .asInstanceOf[ShuffleDependency[_, _, _]]
        val shuffleHandle = dependency.shuffleHandle

        val shuffleExec = exec
          .asInstanceOf[QueryStageExec]
          .plan
          .asInstanceOf[ArrowShuffleExchangeExec]
        val inputMetrics = shuffleExec.metrics
        val inputRDD = exec match {
          case exec: ShuffleQueryStageExec => executeNative(exec.plan)
          case exec: CustomShuffleReaderExec => executeNative(exec.child)
        }

        val nativeSchema = shuffleExec.nativeSchema
        val metrics = inputRDD.metrics
        val partitionClsName = shuffledRDD.getClass.getSimpleName

        new NativeRDD(
          shuffledRDD.sparkContext,
          metrics,
          shuffledRDD.partitions,
          new OneToOneDependency(shuffledRDD) :: Nil,
          true,
          (partition, taskContext) => {
            val shuffleReadMetrics = taskContext.taskMetrics().createTempShuffleReadMetrics()
            val metricsReporter =
              new SQLShuffleReadMetricsReporter(shuffleReadMetrics, inputMetrics)

            val classOfShuffledRowRDDPartition =
              Utils.classForName("org.apache.spark.sql.execution.ShuffledRowRDDPartition")
            val classOfAdaptiveShuffledRowRDDPartition =
              Utils.classForName(
                "org.apache.spark.sql.execution.adaptive.AdaptiveShuffledRowRDDPartition")

            val readers: Iterator[ShuffleReader[_, _]] = shuffledRDD match {
//              case rdd: LocalShuffledRowRDD =>
//                val shuffledRowPartition = partition.asInstanceOf[ShuffledRDDPartition]
//                val mapId = shuffledRowPartition.index
//                val partitionStartIndices = rdd.partitionStartIndices.iterator
//                val partitionEndIndices = rdd.partitionEndIndices.iterator
//                partitionStartIndices
//                  .zip(partitionEndIndices)
//                  .map {
//                    case (start, end) =>
//                      logInfo(
//                        s"Create local shuffle reader mapId $mapId, partition range $start-$end")
//                      SparkEnv.get.shuffleManager
//                        .getReader(
//                          shuffleHandle,
//                          start,
//                          end,
//                          taskContext,
//                          metricsReporter,
//                          mapId,
//                          mapId + 1)
//                  }
              case _ =>
                partition match {
                  case p if classOfShuffledRowRDDPartition.isInstance(p) =>
                    val clz = classOfShuffledRowRDDPartition
                    val startPreShufflePartitionIndex =
                      clz.getMethod("startPreShufflePartitionIndex").invoke(p).asInstanceOf[Int]
                    val endPreShufflePartitionIndex =
                      clz.getMethod("endPreShufflePartitionIndex").invoke(p).asInstanceOf[Int]

                    Iterator.single(
                      SparkEnv.get.shuffleManager
                        .getReader(
                          shuffleHandle,
                          startPreShufflePartitionIndex,
                          endPreShufflePartitionIndex,
                          taskContext,
                          metricsReporter))

                  case p if classOfAdaptiveShuffledRowRDDPartition.isInstance(p) =>
                    val clz = classOfAdaptiveShuffledRowRDDPartition
                    val preShufflePartitionIndex =
                      clz.getMethod("preShufflePartitionIndex").invoke(p).asInstanceOf[Int]
                    val startMapId = clz.getMethod("startMapId").invoke(p).asInstanceOf[Int]
                    val endMapId = clz.getMethod("endMapId").invoke(p).asInstanceOf[Int]

                    Iterator.single(
                      SparkEnv.get.shuffleManager
                        .getReaderForRange(
                          shuffleHandle,
                          preShufflePartitionIndex,
                          preShufflePartitionIndex + 1,
                          startMapId,
                          endMapId,
                          taskContext,
                          metricsReporter))
                  case p =>
                    Iterator.single(
                      SparkEnv.get.shuffleManager.getReader(
                        shuffleHandle,
                        p.index,
                        p.index + 1,
                        taskContext,
                        metricsReporter))
                }
            }

            // store fetch iterator in jni resource before native compute
            val jniResourceId = s"NativeShuffleReadExec:${UUID.randomUUID().toString}"
            JniBridge.resourcesMap.put(
              jniResourceId,
              () => {
                CompletionIterator[Object, Iterator[Object]](
                  readers.flatMap(_.asInstanceOf[ArrowBlockStoreShuffleReader[_, _]].readIpc()),
                  taskContext.taskMetrics().mergeShuffleReadMetrics())
              })

            PhysicalPlanNode
              .newBuilder()
              .setIpcReader(
                IpcReaderExecNode
                  .newBuilder()
                  .setSchema(nativeSchema)
                  .setNumPartitions(shuffledRDD.getNumPartitions)
                  .setIpcProviderResourceId(jniResourceId)
                  .setMode(IpcReadMode.CHANNEL_AND_FILE_SEGMENT)
                  .build())
              .build()
          },
          friendlyName = s"NativeRDD.ShuffleRead [$partitionClsName]")
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "BroadcastExchange does not support the execute() code path.")
  }

  override def output: Seq[Attribute] = {
    throw new UnsupportedOperationException(
      "BroadcastExchange does not support the execute() code path.")
  }

  override def children: Seq[SparkPlan] = {
    throw new UnsupportedOperationException(
      "BroadcastExchange does not support the execute() code path.")
  }
}
