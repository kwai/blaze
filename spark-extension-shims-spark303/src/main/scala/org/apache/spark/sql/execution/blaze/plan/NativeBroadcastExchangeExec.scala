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

import java.util.UUID

import scala.concurrent.Promise

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.execution.SparkPlan

case class NativeBroadcastExchangeExec(mode: BroadcastMode, override val child: SparkPlan)
    extends NativeBroadcastExchangeBase(mode, child) {

  override lazy val runId: UUID = UUID.randomUUID()
  override def getRunId: UUID = runId

  override def runtimeStatistics: Statistics = {
    val dataSize = metrics("dataSize").value
    val rowCount = metrics.getOrElse("Native.output_rows", metrics("output_rows")).value
    Statistics(dataSize, Some(rowCount))
  }

  @transient
  override lazy val completionFuture: concurrent.Future[Broadcast[Any]] = {
    // NativeBroadcastExchangeExec is not materialized until doExecuteBroadcast called
    // so return a dummy future here
    Promise.successful(null).future
  }

  override def withNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
    copy(child = newChildren.head)
}
