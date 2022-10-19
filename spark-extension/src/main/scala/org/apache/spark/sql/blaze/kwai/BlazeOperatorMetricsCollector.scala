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

package org.apache.spark.sql.blaze.kwai

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.sql.blaze.NativeSupports
import org.apache.spark.sql.blaze.NativeSupports.blazeOperatorMetricsCollector
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.{KwaiSparkBasicMetrics, SparkContext, SparkEnv}
import com.fasterxml.jackson.databind.ObjectMapper

import scala.beans.BeanProperty
import scala.util.Try

class BlazeOperatorMetricsCollector() extends Logging {

  private val kafkaStageMetricsBroker =
    SparkEnv.get.conf.get(
      "spark.kwai.blazeOperatorMetrics.broker",
      defaultValue =
        "dataarch-bjdy-rs7110.idczw.hb1.kwaidc.com:9092,dataarch-bjdy-rs7111.idczw.hb1.kwaidc.com:9092,dataarch-bjdy-rs7112.idczw.hb1.kwaidc.com:9092,dataarch-bjdy-rs7113.idczw.hb1.kwaidc.com:9092,dataarch-bjdy-rs7114.idczw.hb1.kwaidc.com:9092")
  private val kafkaStageMetricsTopic =
    SparkEnv.get.conf.get(
      "spark.kwai.blazeOperatorMetrics.topic",
      defaultValue = "dataarch_blaze_operator_metrics")

  private val producer: Option[KafkaClient] = Try(
    KafkaClient(kafkaStageMetricsBroker, null, kafkaStageMetricsTopic)).toOption

  private val objectMapper: ObjectMapper = new ObjectMapper()

  def sendOperatorMetrics(
      stageInfo: StageInfo,
      nodeName: String,
      outPut: Long,
      simpleString: String,
      sc: SparkContext): Unit = {
    val stageId = stageInfo.stageId
    val stageAttemptId = stageInfo.attemptNumber()
    val stageStatus = stageInfo.getStatusString
    val stageFailedReason = if (stageInfo.failureReason.isEmpty) {
      "None"
    } else {
      stageInfo.failureReason.get
    }
    val execName = nodeName
    val execInfo = simpleString
    val outPutRows = outPut
    try {
      val msg = buildMsg(
        sc,
        stageId,
        stageAttemptId,
        execName,
        outPutRows,
        execInfo,
        stageStatus,
        stageFailedReason)
      logInfo("BlazeOperatorMetric: " + msg)
      producer.foreach(_.send(key = sc.conf.getAppId, msg))
    } catch {
      case _: Exception => // ignore exceptions
    }
  }

  private def buildMsg(
      sc: SparkContext,
      stageId: Int,
      stageAttemptId: Int,
      execName: String,
      outPutRows: Long,
      execInfo: String,
      stageStatus: String,
      stageFailedReason: String): String =
    objectMapper.writeValueAsString(
      new BlazeOperatorMetrics(
        sc,
        stageId,
        stageAttemptId,
        execName,
        outPutRows,
        execInfo,
        stageStatus,
        stageFailedReason))

}

object BlazeOperatorMetricsCollector {
  val isBlazeOperatorMetricsenabled: Boolean =
    SparkEnv.get.conf.getBoolean("spark.blaze.enable.operatorMetrics", defaultValue = true)

  def createListener(plan: NativeSupports, sc: SparkContext): Unit = {

    val addListener: LiveListenerBus = sc.listenerBus
    addListener.addToQueue(
      new BlazeOperatorMetricsListener(sc, plan.getClass.getSimpleName, plan, plan.simpleString),
      "BlazeOperatorMetrics")
  }
}

class BlazeOperatorMetrics(
    sc: SparkContext,
    metricStageId: Int,
    metricStageAttemptId: Int,
    execName: String,
    outputRows: Long,
    execInfo: String,
    stageStatus: String,
    stageFailedReason: String)
    extends KwaiSparkBasicMetrics(sc) {
  @BeanProperty val stageId: Int = metricStageId
  @BeanProperty val stageAttemptId: Int = metricStageAttemptId
  @BeanProperty val operatorName: String = execName
  @BeanProperty val operatorOutPutRows: Long = outputRows
  @BeanProperty val operatorInfo: String = execInfo
  @BeanProperty val operatorStageStatus: String = stageStatus
  @BeanProperty val operatorStageFailedReason: String = stageFailedReason
}

class BlazeOperatorMetricsListener(
    sc: SparkContext,
    nodeName: String,
    exec: => NativeSupports,
    simpleString: String)
    extends SparkListener
    with Logging {

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {

    blazeOperatorMetricsCollector match {
      case _: Some[BlazeOperatorMetricsCollector] =>
        val metricsOutput = exec.metrics("output_rows")
        metricsOutput match {
          case _: SQLMetric =>
            blazeOperatorMetricsCollector.get.sendOperatorMetrics(
              stageCompleted.stageInfo,
              nodeName,
              exec.metrics("output_rows").value,
              simpleString,
              sc)
          case _ =>
            blazeOperatorMetricsCollector.get.sendOperatorMetrics(
              stageCompleted.stageInfo,
              nodeName,
              0L,
              simpleString,
              sc)
        }
      case _ =>
    }

    sc.listenerBus.removeListener(this)
  }
}
