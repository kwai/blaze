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

import java.util.concurrent.Future
import java.util.UUID

import scala.compat.java8.FutureConverters._
import scala.concurrent.Promise
import scala.util.control.NonFatal

import org.apache.spark.broadcast
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.blaze.NativeSupports
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.util.SparkFatalException

case class NativeBroadcastExchangeExec(mode: BroadcastMode, override val child: SparkPlan)
    extends NativeBroadcastExchangeBase(mode, child)
    with NativeSupports {

  override def getRunId: UUID = UUID.randomUUID()

  override def runtimeStatistics: Statistics = {
    val dataSize = metrics("dataSize").value
    val rowCount = metrics("numOutputRows").value
    Statistics(dataSize, Some(rowCount))
  }

  @transient
  private lazy val promise = Promise[broadcast.Broadcast[Any]]()

  @transient
  override lazy val completionFuture: scala.concurrent.Future[broadcast.Broadcast[Any]] =
    promise.future

  @transient
  override lazy val relationFuture: Future[Broadcast[Any]] = {
    if (isNative) {
      nativeRelationFuture.asInstanceOf[Future[Broadcast[Any]]]
    } else {
      promise
        .completeWith(scala.concurrent.Future {
          scala.concurrent.blocking {
            nonNativeBroadcastExec.relationFuture.get
          }
        }(BroadcastExchangeExec.executionContext))
        .future
        .toJava
        .toCompletableFuture
    }
  }

  @transient
  override lazy val nativeRelationFuture: Future[Broadcast[Array[Array[Byte]]]] = {
    SQLExecution.withThreadLocalCaptured[Broadcast[Array[Array[Byte]]]](
      this.session.sqlContext.sparkSession,
      BroadcastExchangeExec.executionContext) {
      try {
        sparkContext.setJobGroup(
          getRunId.toString,
          s"native broadcast exchange (runId $getRunId)",
          interruptOnCancel = true)

        val collected = collectNative()
        logInfo(s"Blaze broadcast collectNative.bytes=${collected.map(_.length).sum}")

        val broadcasted = sparkContext.broadcast(collected)
        promise.trySuccess(broadcasted.asInstanceOf[Broadcast[Any]])
        broadcasted
      } catch {
        // SPARK-24294: To bypass scala bug: https://github.com/scala/bug/issues/9554, we throw
        // SparkFatalException, which is a subclass of Exception. ThreadUtils.awaitResult
        // will catch this exception and re-throw the wrapped fatal throwable.
        case oe: OutOfMemoryError =>
          val ex = new SparkFatalException(
            QueryExecutionErrors.notEnoughMemoryToBuildAndBroadcastTableError(oe))
          promise.tryFailure(ex)
          throw ex
        case e if !NonFatal(e) =>
          val ex = new SparkFatalException(e)
          promise.tryFailure(ex)
          throw ex
        case e: Throwable =>
          promise.tryFailure(e)
          throw e
      }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}
