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
package org.apache.spark.sql.blaze

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.metric.SQLMetric

case class MetricNode(
    metrics: Map[String, SQLMetric],
    children: Seq[MetricNode],
    metricValueHandler: Option[(String, Long) => Unit] = None)
    extends Logging {

  def getChild(i: Int): MetricNode =
    children(i)

  def add(metricName: String, v: Long): Unit = {
    metricValueHandler.foreach(_.apply(metricName, v))
    if (v > 0) {
      metrics.get(metricName).foreach(_.add(v))
    }
  }

  def foreach(fn: MetricNode => Unit): Unit = {
    fn(this)
    this.children.foreach(_.foreach(fn))
  }
}
