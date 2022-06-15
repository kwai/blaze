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

package org.apache.spark.sql.blaze.execution

import java.util.UUID
import java.util.concurrent.Future

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.blaze.NativeRDD
import org.apache.spark.sql.blaze.NativeSupports
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.plans.physical.Distribution
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.BroadcastExchangeLike
import org.apache.spark.InterruptibleIterator
import org.apache.spark.sql.blaze.JniBridge
import org.apache.spark.sql.blaze.MetricNode
import org.apache.spark.sql.blaze.NativeConverters
import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.catalyst.plans.physical.IdentityBroadcastMode
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.Partition
import org.blaze.protobuf.PhysicalPlanNode
import org.blaze.protobuf.Schema
import org.blaze.protobuf.ShuffleReaderExecNode

case class ArrowBroadcastExchangeExec(mode: BroadcastMode, override val child: SparkPlan)
    extends BroadcastExchangeLike
    with NativeSupports {

  private val originalBroadcastExec = BroadcastExchangeExec(mode, child)
  private val identityBroadcastExec = BroadcastExchangeExec(IdentityBroadcastMode, child)
  private def broadcastExec: BroadcastExchangeExec =
    getTagValue(ArrowBroadcastExchangeExec.nativeExecutionTag) match {
      case Some(true) => identityBroadcastExec
      case _ => originalBroadcastExec
    }

  override def runId: UUID = originalBroadcastExec.runId

  override def logicalLink: Option[LogicalPlan] = originalBroadcastExec.logicalLink
  override def output: Seq[Attribute] = originalBroadcastExec.output
  override def outputPartitioning: Partitioning =
    new Partitioning() {
      override val numPartitions: Int = 1
      override def satisfies0(required: Distribution): Boolean = true
    }

  override lazy val metrics: Map[String, SQLMetric] = {
    NativeSupports.getDefaultNativeMetrics(sparkContext) ++ broadcastExec.metrics
  }

  override def doPrepare(): Unit = relationFuture

  override def doExecuteBroadcast[T](): Broadcast[T] =
    broadcastExec.doExecuteBroadcast()

  override def doExecuteNative(): NativeRDD = {
    val nativeMetrics = MetricNode(metrics, Nil)
    val timeZoneId = SparkEnv.get.conf.get(SQLConf.SESSION_LOCAL_TIMEZONE)
    val broadcast = identityBroadcastExec.executeBroadcast[Array[InternalRow]]()
    val partitions = Array(new Partition() {
      override def index: Int = 0
    })

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      partitions,
      Nil,
      (_, context) => {
        val inputRows = broadcast.value
        val resourceId = "ArrowBroadcastExchangeExec" +
          s":stage=${context.stageId()}" +
          s":partition=${context.partitionId()}" +
          s":taskAttempt=${context.taskAttemptId()}" +
          s":uuid=${UUID.randomUUID().toString}"

        val provideIpcIterator = () => {
          val inputRowIter = inputRows.iterator
          val ipcIterator = new ArrowWriterIterator(inputRowIter, schema, timeZoneId, context)
          new InterruptibleIterator(context, ipcIterator)
        }
        JniBridge.resourcesMap.put(resourceId, () => provideIpcIterator())
        PhysicalPlanNode
          .newBuilder()
          .setShuffleReader(
            ShuffleReaderExecNode
              .newBuilder()
              .setSchema(nativeSchema)
              .setNumPartitions(1)
              .setNativeShuffleId(resourceId)
              .build())
          .build()
      })
  }

  val nativeSchema: Schema = NativeConverters.convertSchema(
    StructType(output.map(a => StructField(a.toString(), a.dataType, a.nullable, a.metadata))))

  @transient
  override lazy val relationFuture: Future[Broadcast[Any]] = broadcastExec.relationFuture

  @transient
  override lazy val completionFuture: concurrent.Future[Broadcast[Any]] =
    broadcastExec.completionFuture

  override def runtimeStatistics: Statistics = broadcastExec.runtimeStatistics

  override def withNewChildren(children: Seq[SparkPlan]): ArrowBroadcastExchangeExec = {
    ArrowBroadcastExchangeExec(mode, children.head)
  }
}

object ArrowBroadcastExchangeExec {
  def nativeExecutionTag: TreeNodeTag[Boolean] = TreeNodeTag("arrowBroadcastNativeExecution")
}
