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

import scala.beans.BeanProperty
import scala.util.Try

import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.{KwaiSparkBasicMetrics, SparkContext, SparkEnv}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.spark.internal.Logging
import org.apache.spark.sql.blaze.kwai.BlazeOperatorMetricsCollector.planStore
import org.codehaus.jackson.annotate.JsonProperty
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

import org.apache.spark.sql.blaze.Shims

class BlazeOperatorMetricsCollector extends Logging {

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

  def sendOperatorMetrics(sc: SparkContext): Unit = {
    try {
      val startTime = System.currentTimeMillis()
      val operatorList = new ListBuffer[BlazeOperatorInfo]()

      planStore.zipWithIndex.foreach { case (plan, index) =>
        val metricObject = objectMapper.createObjectNode()
        plan.metrics.foreach { case (key, value) =>
          metricObject.put(key, if (value.value < 0) 0L else value.value)
        }
        operatorList.append(
          BlazeOperatorInfo(
            plan.getClass.getSimpleName,
            index,
            Shims.get.isNative(plan),
            metricObject))
      }
      producer.foreach(
        _.send(
          key = sc.conf.getAppId,
          objectMapper.writeValueAsString(new BlazeApplicationInfo(sc, operatorList.toArray))))
      planStore.clear()
      logInfo(
        s"send app operator metrics to kafka succ after ${System.currentTimeMillis() - startTime}ms")
    } catch {
      case e: Exception =>
        logInfo(s"Blaze sendOperatorMetrics error: ${e.getMessage}")
        producer.foreach(_.flush())
        producer.foreach(_.close())
    }
  }
}

object BlazeOperatorMetricsCollector {
  val isBlazeOperatorMetricsEnabled: Boolean =
    SparkEnv.get.conf.getBoolean("spark.blaze.enable.operatorMetrics", defaultValue = true)

  val instance: Option[BlazeOperatorMetricsCollector] = {
    if (isBlazeOperatorMetricsEnabled) {
      Some(new BlazeOperatorMetricsCollector)
    } else {
      None
    }
  }
  var planStore: ArrayBuffer[SparkPlan] = new ArrayBuffer[SparkPlan]()
}

class BlazeApplicationInfo(sc: SparkContext, info: Array[BlazeOperatorInfo])
    extends KwaiSparkBasicMetrics(sc) {
  @BeanProperty
  @JsonProperty("blazeJarVersion")
  val blazeJarVersion: String = sc.conf.get("spark.blaze.jar", "default")
  @BeanProperty
  @JsonProperty("operatorsInfo")
  val operatorsInfo: Array[BlazeOperatorInfo] = info
}

case class BlazeOperatorInfo(
    @BeanProperty
    @JsonProperty("execName")
    execName: String,
    @BeanProperty
    @JsonProperty("execId")
    execId: Int,
    @BeanProperty
    @JsonProperty("isNative")
    isNative: Boolean,
    @BeanProperty
    @JsonProperty("execMetric")
    execMetric: ObjectNode) {}

class OperatorMetricsListener(sc: SparkContext) extends SparkListener with Logging {

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    BlazeOperatorMetricsCollector.instance.foreach { collector =>
      collector.sendOperatorMetrics(sc)
    }
    sc.listenerBus.removeListener(this)
  }
}
