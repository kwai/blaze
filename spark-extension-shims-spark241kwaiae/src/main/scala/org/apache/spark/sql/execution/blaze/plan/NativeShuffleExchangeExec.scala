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
import org.apache.spark._
import org.apache.spark.rdd.MapPartitionsRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.shuffle.ShuffleWriteProcessor
import org.apache.spark.shuffle.ShuffleWriter
import org.apache.spark.sql.blaze.NativeHelper
import org.apache.spark.sql.blaze.NativeRDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors.attachTree
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.metric.SQLShuffleReadMetricsReporter
import org.apache.spark.sql.execution.metric.SQLShuffleWriteMetricsReporter
import org.apache.spark.sql.execution.SQLExecution.EXECUTION_ID_KEY
import org.apache.spark.sql.execution.blaze.shuffle.{BlazeShuffleWriter, BlazeRssShuffleWriter}
import org.apache.spark.sql.internal.SQLConf

case class NativeShuffleExchangeExec(
    var newPartitioning: Partitioning,
    override val child: SparkPlan)
    extends NativeShuffleExchangeBase(newPartitioning, child) {

  val shuffleDescStr =
    s"$newPartitioning[${child.output.map(_.dataType.simpleString).mkString(",")}]"

  // NOTE: coordinator can be null after serialization/deserialization,
  //       e.g. it can be null on the Executor side
  override lazy val writeMetrics: Map[String, SQLMetric] = (mutable.LinkedHashMap[String, SQLMetric]() ++
    SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext, shuffleDescStr) ++
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
            "disk_spill_iotime",
            "shuffle_read_total_time"))
        .toSeq: _*)).toMap

  override lazy val readMetrics: Map[String, SQLMetric] =
    SQLShuffleReadMetricsReporter.createShuffleReadMetrics(sparkContext, shuffleDescStr)

  override lazy val metrics: Map[String, SQLMetric] =
    (mutable.LinkedHashMap[String, SQLMetric]() ++
      readMetrics ++
      writeMetrics ++
      Map(
        "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
        "numPartitions" -> SQLMetrics.createMetric(sparkContext, "number of partitions"))).toMap

  private def preparePostShuffleRDD(
      shuffleDependency: ShuffleDependency[Int, InternalRow, InternalRow],
      specifiedPartitionStartIndices: Option[Array[Int]] = None): ShuffledRowRDD = {
    // If an array of partition start indices is provided, we need to use this array
    // to create the ShuffledRowRDD. Also, we need to update outputPartitioning to
    // update the number of post-shuffle partitions.
    specifiedPartitionStartIndices.foreach { indices =>
      assert(newPartitioning.isInstanceOf[HashPartitioning])
      newPartitioning = UnknownPartitioning(indices.length)
    }
    new ShuffledRowRDD(shuffleDependency, readMetrics, specifiedPartitionStartIndices)
  }

  /**
   * Caches the created ShuffleRowRDD so we can reuse that.
   */
  private var cachedShuffleRDD: ShuffledRowRDD = _

  protected override def doExecuteNonNative(): RDD[InternalRow] =
    attachTree(this, "execute") {
      // Returns the same ShuffleRowRDD if this plan is used by multiple plans.
      if (cachedShuffleRDD == null) {
        cachedShuffleRDD = preparePostShuffleRDD(shuffleDependency)
      }
      cachedShuffleRDD
    }

  private var _mapOutputStatistics: MapOutputStatistics = _

  override def mapOutputStatistics: MapOutputStatistics = _mapOutputStatistics

  override def eagerExecute(): RDD[InternalRow] = {
    if (cachedShuffleRDD == null) {
      shuffleDependency.rdd.stageId = this.stageId
      val allOperatorMetrics = new mutable.HashSet[SQLMetric]()
      metrics.foreach { case (_: String, v: SQLMetric) =>
        allOperatorMetrics.add(v)
      }
      transformUp { case p: SparkPlan =>
        p.metrics.foreach { case (_: String, sqlMetric: SQLMetric) =>
          allOperatorMetrics.add(sqlMetric)
        }
        p
      }
      val executionId = sqlContext.sparkContext.getLocalProperty(EXECUTION_ID_KEY)
      SQLMetrics.postDriverMetricUpdates(
        sqlContext.sparkContext,
        executionId,
        allOperatorMetrics.toSeq)
      if (shuffleDependency.rdd.partitions.length != 0) {
        // submitMapStage does not accept RDD with 0 partition.
        // So, we will not submit this dependency.
        val submittedStageFuture = sqlContext.sparkContext.submitMapStage(shuffleDependency)
        try {
          val stat = submittedStageFuture.get()
          val totalBytesWritten = stat.bytesByPartitionId.sum
          var dataSizeFactor = 1.0

          val numRecordsWritten = metrics(
            SQLShuffleWriteMetricsReporter.SHUFFLE_RECORDS_WRITTEN).value

          if (numRecordsWritten > 0 && totalBytesWritten > 0) {
            val broadcastCountLimit = SQLConf.get.broadcastCountLimit() / 4
            val broadcastThreshold = SQLConf.get.getConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD)
            val dataSize = metrics("dataSize").value

            if (totalBytesWritten.toDouble <= broadcastThreshold) {
              if (numRecordsWritten >= broadcastCountLimit) {
                // NOTE:
                //  in some cases, the number of written records exceeds broadcastCountLimit
                // but the size is smaller than autoBroadcastThreshold. in this situation
                // spark incorrectly turns SMJ into BHJ and always fails the broadcast. so we
                // have to manually increase the stats by setting the dataSizeFactor.
                dataSizeFactor = broadcastThreshold / totalBytesWritten.toDouble + 0.1
              }
            } else {
              // NOTE:
              //  in some cases, the compression ratio of vectorized data is too high, resulting
              // the number of tasks in next stage is too small.
              // increase the dataSizeFactor to avoid this.
              val minCompressionRatio = sparkContext.conf
                .getDouble("spark.blaze.shuffle.minCompressRatio", defaultValue = 0.1)
              val realCompressionRatio = totalBytesWritten.toDouble / dataSize
              if (realCompressionRatio < minCompressionRatio) {
                dataSizeFactor = minCompressionRatio / realCompressionRatio
              }
            }
          }

          logInfo(
            s"shuffleId=${shuffleDependency.shuffleId}" +
              s", numRecordsWritten=$numRecordsWritten" +
              s", totalBytesWritten=$totalBytesWritten")
          _mapOutputStatistics = new MapOutputStatistics(
            stat.shuffleId,
            stat.bytesByPartitionId.map(n => (n * dataSizeFactor).ceil.toLong),
            stat.recordsByPartitionId)

        } catch {
          case throwable: Throwable =>
            sparkContext.dagScheduler.sendFailedReason(throwable)
            throw throwable
        }
      }
      cachedShuffleRDD = preparePostShuffleRDD(shuffleDependency)
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
          mapId: Int,
          context: TaskContext,
          partition: Partition): MapStatus = {

        val manager = SparkEnv.get.shuffleManager
        var writer: ShuffleWriter[Any, Any] = null
        writer = manager.getWriter[Any, Any](
          dep.shuffleHandle,
          mapId,
          context,
          createMetricsReporter(context))
        if (SparkEnv.get.conf.get("spark.shuffle.manager", "sort").equals("unify")) {
          writer
            .asInstanceOf[BlazeRssShuffleWriter[_, _]]
            .nativeRssShuffleWrite(
              rdd.asInstanceOf[MapPartitionsRDD[_, _]].prev.asInstanceOf[NativeRDD],
              dep,
              mapId,
              context,
              partition,
              numPartitions)
        } else {
          writer
            .asInstanceOf[BlazeShuffleWriter[_, _]]
            .nativeShuffleWrite(
              rdd.asInstanceOf[MapPartitionsRDD[_, _]].prev.asInstanceOf[NativeRDD],
              dep,
              mapId,
              context,
              partition)
        }
      }
    }
  }
}
