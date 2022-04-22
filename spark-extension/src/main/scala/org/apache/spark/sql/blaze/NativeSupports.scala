package org.apache.spark.sql.blaze

import scala.annotation.tailrec

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.adaptive.CustomShuffleReaderExec
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.TaskContext
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.SparkEnv
import org.blaze.protobuf.PartitionId
import org.blaze.protobuf.PhysicalPlanNode
import org.blaze.protobuf.TaskDefinition

trait NativeSupports {
  def doExecuteNative(): NativeRDD
}

object NativeSupports extends Logging {
  @tailrec def isNative(plan: SparkPlan): Boolean = plan match {
    case _: NativeSupports => true
    case plan: CustomShuffleReaderExec => isNative(plan.child)
    case plan: QueryStageExec => isNative(plan.plan)
    case _ => false
  }

  @tailrec def executeNative(plan: SparkPlan): NativeRDD = plan match {
    case plan: NativeSupports => plan.doExecuteNative()
    case plan: CustomShuffleReaderExec => executeNative(plan.child)
    case plan: QueryStageExec => executeNative(plan.plan)
    case _ => throw new SparkException(s"Underlying plan is not NativeSupports: ${plan}")
  }

  def executeNativePlan(
      nativePlan: PhysicalPlanNode,
      metrics: MetricNode,
      context: TaskContext): Iterator[InternalRow] = {

    val partitionId = PartitionId
      .newBuilder()
      .setPartitionId(context.partitionId())
      .setStageId(context.stageId())
      .setJobId(context.partitionId().toString)
      .build()

    val taskDefinition = TaskDefinition
      .newBuilder()
      .setTaskId(partitionId)
      .setPlan(nativePlan)
      .build()

    // note: consider passing a ByteBufferOutputStream to blaze-rs to avoid copying
    if (SparkEnv.get.conf.getBoolean("spark.blaze.dumpNativePlanBeforeExecuting", false)) {
      logInfo(s"Start executing native plan: ${taskDefinition.toString}")
    } else {
      logInfo(s"Start executing native plan")
    }

    val nativeMemory = SparkEnv.get.conf
      .getLong("spark.executor.memoryOverhead", Long.MaxValue) * 1024 * 1024
    val memoryFraction = SparkEnv.get.conf.getDouble("spark.blaze.memoryFraction", 0.75)
    val batchSize = SparkEnv.get.conf.getLong("spark.blaze.batchSize", 16384)
    val tokioPoolSize = SparkEnv.get.conf.getLong("spark.blaze.tokioPoolSize", 10)
    val tmpDirs = SparkEnv.get.blockManager.diskBlockManager.localDirsString.mkString(",")

    val iterPtr = JniBridge.callNative(
      taskDefinition.toByteArray,
      tokioPoolSize,
      batchSize,
      nativeMemory,
      memoryFraction,
      tmpDirs,
      metrics)

    if (iterPtr < 0) {
      logWarning("Error occurred while call physical_plan.execute")
      return Iterator.empty
    }

    FFIHelper.fromBlazeIter(iterPtr, context)
  }

  def getDefaultNativeMetrics(sparkContext: SparkContext): Map[String, SQLMetric] =
    Map(
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "blazeExecIPCWrittenRows" -> SQLMetrics
        .createMetric(sparkContext, "blaze exec IPC written rows"),
      "blazeExecIPCWrittenBytes" -> SQLMetrics
        .createSizeMetric(sparkContext, "blaze exec IPC written bytes"),
      "blazeExecTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "blaze exec time"),
      "blazeShuffleWriteExecTime" -> SQLMetrics
        .createNanoTimingMetric(sparkContext, "blaze shuffle write exec time"))
}

case class MetricNode(metrics: Map[String, SQLMetric], children: Seq[MetricNode]) {
  def getChild(i: Int): MetricNode =
    children(i)

  def add(metricName: String, v: Long): Unit =
    metrics.get(metricName).foreach(_.add(v))
}
