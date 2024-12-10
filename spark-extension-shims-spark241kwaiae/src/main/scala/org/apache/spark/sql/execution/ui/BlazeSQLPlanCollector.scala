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
package org.apache.spark.sql.execution.ui

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.blaze.plan.NativeParquetScanExec
import org.apache.spark.util.Utils
import org.apache.spark.{KwaiSparkBasicMetrics, SparkContext}

import java.text.NumberFormat
import java.util.Locale
import java.util.regex.Pattern
import scala.beans.BeanProperty
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

class BlazeSQLPlanCollector(sc: SparkContext, statusStore: SQLAppStatusStore)
    extends SparkListener
    with Logging {

  private val kafkaMetricsBroker =
    sc.conf.get(
      "spark.kwai.blazeOperatorMetrics.broker",
      defaultValue =
        "dataarch-bjdy-rs7110.idczw.hb1.kwaidc.com:9092,dataarch-bjdy-rs7111.idczw.hb1.kwaidc.com:9092,dataarch-bjdy-rs7112.idczw.hb1.kwaidc.com:9092,dataarch-bjdy-rs7113.idczw.hb1.kwaidc.com:9092,dataarch-bjdy-rs7114.idczw.hb1.kwaidc.com:9092")
  private val kafkaMetricsTopic =
    sc.conf.get(
      "spark.kwai.blazeOperatorMetrics.topic",
      defaultValue = "dataarch_blaze_operator_metrics")

  private val producer: Option[KafkaClient] = Try(
    KafkaClient(kafkaMetricsBroker, null, kafkaMetricsTopic)).toOption

  private val objectMapper: ObjectMapper = new ObjectMapper()

  def buildExecutionPlansInfo(executionId: Long): Unit = {
    try {
      val executionMetrics = statusStore.executionMetrics(executionId)
      statusStore.planGraph(executionId).nodes.foreach { plan =>
        BlazeSQLPlanCollector.plansInfo.append(buildPlanInfo(executionId, executionMetrics, plan))
      }
    } catch {
      case e: Exception => // does not fail when proceed a metric error, ignore
    }
  }

  def sendPlanMetrics2Kafka(): Unit = {
    try {
      val startTime = System.currentTimeMillis()
      producer.foreach(
        _.send(
          key = sc.conf.getAppId,
          objectMapper.writeValueAsString(
            new BlazeApplicationInfo(sc, BlazeSQLPlanCollector.plansInfo.toArray))))
      logInfo(
        s"send app opretor metrics to kafka succ after ${System.currentTimeMillis() - startTime}ms")
    } catch {
      case e: Exception =>
        logInfo(s"Blaze sendOperatorMetrics error: ${e.getMessage}")
        producer.foreach(_.flush())
        producer.foreach(_.close())
    }
  }

  private def buildPlanInfo(
      executionId: Long,
      executionMetrics: Map[Long, String],
      plan: SparkPlanGraphNode): BlazeOperatorInfo = {
    val metricObject = objectMapper.createObjectNode()
    metricObject.put("planIsOverride", 1)
    plan.metrics.foreach { metric =>
      try {
        executionMetrics.get(metric.accumulatorId).foreach { rawMetricValue =>
          parsePlanMetric(metric, rawMetricValue).foreach { case (metricName, metricValue) =>
            metricObject.put(metricName, metricValue)
          }
        }
      } catch {
        case e: Exception =>
          // does not fail when sending a metric error, ignore
          logInfo(s"get metric ${metric.name} error because ${e.getMessage}")
      }
    }
    new BlazeOperatorInfo(executionId, plan, metricObject)
  }

  private def parsePlanMetric(metric: SQLPlanMetric, metricValue: String): Seq[(String, Long)] = {
    val metricName = metric.name
    metric.metricType match {
      case "sum" =>
        val valueParser: (String => Number) = NumberFormat.getNumberInstance(Locale.US).parse
        if (metric.name.equals("stageId")) {
          Seq(
            (metricName, valueParser(metricValue).longValue()),
            (
              "stage_num_tasks",
              BlazeSQLPlanCollector.stageInfo
                .getOrElse(valueParser(metricValue).intValue(), (-1L, 0L))
                ._2),
            (
              "is_stage_failed",
              BlazeSQLPlanCollector.stageInfo
                .getOrElse(valueParser(metricValue).intValue(), (-1L, 0L))
                ._1))
        } else {
          Seq((metricName, valueParser(metricValue).longValue()))
        }

      case "size" | "timing" | "nsTiming" =>
        val sumMinMedMaxPattern = "(.*?)\\((.*?),(.*?),(.*?)\\)".r
        val valueParser: (String => Double) = metric.metricType match {
          case "size" => sizeStringToBytes
          case "timing" | "nsTiming" => timeStringToMsDuration
        }
        val deleteSpaceValue = metricValue.replaceAll("\\s", "")
        deleteSpaceValue match {
          case sumMinMedMaxPattern(sum, min, med, max) =>
            val frontPattern = metricName.replaceAll("\\(.*?\\)|\\s|(?i)total", "")
            Seq(
              (frontPattern + "_sum", valueParser(sum.split("\\s").mkString).longValue()),
              (frontPattern + "_min", valueParser(min.split("\\s").mkString).longValue()),
              (frontPattern + "_med", valueParser(med.split("\\s").mkString).longValue()),
              (frontPattern + "_max", valueParser(max.split("\\s").mkString).longValue()))
          case _ => Seq() // cannot parse, ignore
        }

      case "average" =>
        val minMedMaxPattern = "\n\\((.*?), (.*?), (.*?)\\)".r
        val valueParser: (String => Number) = NumberFormat.getNumberInstance(Locale.US).parse
        metricValue match {
          case minMedMaxPattern(max, med, min) =>
            val frontPattern = metricName.replaceAll("\\(.*?\\)|\\s|(?i)total", "")
            Seq(
              (frontPattern + "_min", valueParser(min).longValue()),
              (frontPattern + "_med", valueParser(med).longValue()),
              (frontPattern + "_max", valueParser(max).longValue()))
          case _ => Seq() // cannot parse, ignore
        }

      case _ => Seq() // unrecognized metric type, ignore
    }
  }

  override def close(): Unit = {
    producer.foreach(_.flush())
    producer.foreach(_.close())
  }

  def sizeStringToBytes(size: String): Double = {
    val EB = 1L << 60
    val PB = 1L << 50
    val TB = 1L << 40
    val GB = 1L << 30
    val MB = 1L << 20
    val KB = 1L << 10
    val B = 1L

    val upper = size.toUpperCase(Locale.ROOT).trim()
    try {
      val matcher = Pattern.compile("(\\d+(?:\\.\\d+)?)([A-Z]+)?").matcher(upper)
      if (matcher.matches()) {
        val value = matcher.group(1).toDouble
        val suffix = matcher.group(2)
        suffix match {
          case "EB" => value * EB
          case "PB" => value * PB
          case "TB" => value * TB
          case "GB" => value * GB
          case "MB" => value * MB
          case "KB" => value * KB
          case "B" => value * B
          case _ => value
        }
      } else {
        throw new NumberFormatException("Failed to parse byte string: " + size)
      }
    } catch {
      case _ => throw new NumberFormatException("Failed to parse byte string: " + size)
    }
  }

  def timeStringToMsDuration(time: String): Double = {
    val ms = 1L
    val s = 1000 * ms
    val m = 60 * s
    val min = m
    val h = 60 * m
    val d = 24 * h

    val lower = time.toLowerCase(Locale.ROOT).trim()
    try {
      val matcher = Pattern.compile("(\\d+(?:\\.\\d+)?)([a-z]+)?").matcher(lower)
      if (matcher.matches()) {
        val value = matcher.group(1).toDouble
        val suffix = matcher.group(2)
        suffix match {
          case "ms" => value * ms
          case "s" => value * s
          case "m" => value * m
          case "min" => value * min
          case "h" => value * h
          case "d" => value * d
          case _ => value
        }
      } else {
        throw new NumberFormatException("Failed to parse time string: " + time)
      }
    } catch {
      case _ => {
        throw new NumberFormatException("Failed to parse time string: " + time)
      }
    }
  }

}

class BlazeApplicationInfo(sc: SparkContext, info: Array[BlazeOperatorInfo])
    extends KwaiSparkBasicMetrics(sc) {
  @BeanProperty
  @JsonProperty("blazeJarVersion")
  val blazeJarVersion: String = sc.conf.get("spark.blaze.jar", "nonblaze")
  @BeanProperty
  @JsonProperty("operatorsInfo")
  val operatorsInfo: Array[BlazeOperatorInfo] = info
}

class BlazeOperatorInfo(eid: Long, plan: SparkPlanGraphNode, execMetric: ObjectNode) {
  @BeanProperty
  @JsonProperty("executionId")
  val executionId: Long = eid
  @BeanProperty
  @JsonProperty("planId")
  val planId: Long = plan.id
  @BeanProperty
  @JsonProperty("planName")
  val planName: String = if (plan.name.contains("NativeParquetScan")) {
    Utils.getSimpleName(NativeParquetScanExec.getClass)
  } else {
    plan.name
  }
  @BeanProperty
  @JsonProperty("planMetric")
  val planMetric: ObjectNode = execMetric
}

object BlazeSQLPlanCollector {
  var stageInfo = scala.collection.mutable.Map[Int, (Long, Long)]()
  var plansInfo = new ArrayBuffer[BlazeOperatorInfo]()
}

class BlazePlanListener(sparkContext: SparkContext, statusStore: SQLAppStatusStore)
    extends SparkListener
    with Logging {

  val metricsCollector: BlazeSQLPlanCollector =
    new BlazeSQLPlanCollector(sparkContext, statusStore)

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e: SparkListenerSQLExecutionEnd => onExecutionEnd(e)
    case _ => // Ignore
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val info = stageCompleted.stageInfo
    BlazeSQLPlanCollector.stageInfo.put(
      info.stageId,
      (info.getStatusString.equals("failed").compare(false), info.numTasks))
  }

  private def onExecutionEnd(event: SparkListenerSQLExecutionEnd): Unit = {
    metricsCollector.buildExecutionPlansInfo(event.executionId)
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    metricsCollector.sendPlanMetrics2Kafka()
  }

  override def close(): Unit = {
    sparkContext.listenerBus.removeListener(this)
    metricsCollector.close()
    BlazeSQLPlanCollector.stageInfo.clear()
    BlazeSQLPlanCollector.plansInfo.clear()
  }
}
