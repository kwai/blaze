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

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import org.apache.spark._
import org.apache.spark.rdd.MapPartitionsRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{ShuffleWriteMetricsReporter, ShuffleWriteProcessor, ShuffleWriter}
import org.apache.spark.sql.blaze.NativeHelper
import org.apache.spark.sql.blaze.NativeRDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.blaze.shuffle.BlazeShuffleWriter
import org.apache.spark.sql.execution.exchange.ENSURE_REQUIREMENTS
import org.apache.spark.sql.execution.exchange.ShuffleOrigin
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.metric.SQLShuffleReadMetricsReporter
import org.apache.spark.sql.execution.metric.SQLShuffleWriteMetricsReporter

case class NativeShuffleExchangeExec(
    override val outputPartitioning: Partitioning,
    override val child: SparkPlan)
    extends NativeShuffleExchangeBase(outputPartitioning, child) {

  // NOTE: coordinator can be null after serialization/deserialization,
  //       e.g. it can be null on the Executor side
  lazy val writeMetrics: Map[String, SQLMetric] = (mutable.LinkedHashMap[String, SQLMetric]() ++
    SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext) ++
    mutable.LinkedHashMap(
      NativeHelper
        .getDefaultNativeMetrics(sparkContext)
        .filterKeys(
          Set(
            "stage_id",
            "mem_spill_count",
            "mem_spill_size",
            "mem_spill_iotime",
            "disk_spill_size",
            "disk_spill_iotime"))
        .toSeq: _*)).toMap

  lazy val readMetrics: Map[String, SQLMetric] =
    SQLShuffleReadMetricsReporter.createShuffleReadMetrics(sparkContext)

  override lazy val metrics: Map[String, SQLMetric] =
    (mutable.LinkedHashMap[String, SQLMetric]() ++
      readMetrics ++
      writeMetrics ++
      Map("dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"))).toMap

  Math.max(child.outputPartitioning.numPartitions * outputPartitioning.numPartitions, 1)

  // 'mapOutputStatisticsFuture' is only needed when enable AQE.
  @transient override lazy val mapOutputStatisticsFuture: Future[MapOutputStatistics] = {
    if (inputRDD.getNumPartitions == 0) {
      Future.successful(null)
    } else {
      sparkContext
        .submitMapStage(shuffleDependency)
        .map(stat => new MapOutputStatistics(stat.shuffleId, stat.bytesByPartitionId))
    }
  }

  override def numMappers: Int = shuffleDependency.rdd.getNumPartitions

  override def numPartitions: Int = shuffleDependency.partitioner.numPartitions

  override def getShuffleRDD(partitionSpecs: Array[ShufflePartitionSpec]): RDD[InternalRow] = {
    new ShuffledRowRDD(shuffleDependency, readMetrics, partitionSpecs)
  }

  override def runtimeStatistics: Statistics = {
    val dataSize = metrics("dataSize").value
    val rowCount = metrics(SQLShuffleWriteMetricsReporter.SHUFFLE_RECORDS_WRITTEN).value
    Statistics(dataSize, Some(rowCount))
  }

  /**
   * Caches the created ShuffleRowRDD so we can reuse that.
   */
  private var cachedShuffleRDD: ShuffledRowRDD = _

  protected override def doExecuteNonNative(): RDD[InternalRow] = {
    // Returns the same ShuffleRowRDD if this plan is used by multiple plans.
    if (cachedShuffleRDD == null) {
      cachedShuffleRDD = new ShuffledRowRDD(shuffleDependency, readMetrics)
    }
    cachedShuffleRDD
  }

  override def createNativeShuffleWriteProcessor(
      metrics: Map[String, SQLMetric],
      numPartitions: Int): ShuffleWriteProcessor = {

    new ShuffleWriteProcessor {
      override protected def createMetricsReporter(
          context: TaskContext): ShuffleWriteMetricsReporter = {
        new SQLShuffleWriteMetricsReporter(context.taskMetrics().shuffleWriteMetrics, metrics)
      }

      override def write(
          rdd: RDD[_],
          dep: ShuffleDependency[_, _, _],
          mapId: Long,
          context: TaskContext,
          partition: Partition): MapStatus = {

        val manager = SparkEnv.get.shuffleManager
        var writer: ShuffleWriter[Any, Any] = null
        writer = manager.getWriter[Any, Any](
          dep.shuffleHandle,
          mapId,
          context,
          createMetricsReporter(context))
        writer
          .asInstanceOf[BlazeShuffleWriter[_, _]]
          .nativeShuffleWrite(
            rdd.asInstanceOf[MapPartitionsRDD[_, _]].prev.asInstanceOf[NativeRDD],
            dep,
            mapId.toInt,
            context,
            partition)
      }
    }
  }

  override def shuffleOrigin: ShuffleOrigin = ENSURE_REQUIREMENTS

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}
