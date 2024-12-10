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

import org.apache.commons.lang3.reflect.MethodUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.execution.SparkPlan

case class NativeBroadcastExchangeExec(mode: BroadcastMode, override val child: SparkPlan)
    extends NativeBroadcastExchangeBase(mode, child) {

  private val runId: UUID = UUID.randomUUID()
  override def getRunId: UUID = runId

  override def doExecuteBroadcast[T](): Broadcast[T] = {
    val broadcast = super.doExecuteBroadcast[Any]()
    val v = broadcast.value

    val dummyBroadcasted = new Broadcast[Any](-1) {
      override protected def getValue(): Any = (v, 0, 0, 0, 0) // modified in spark241kwaiae

      override def setValue(v: () => (Any, Long => Any)): Any =
        throw new UnsupportedOperationException("dummyBroadcasted.setValue")

      override protected def doUnpersist(blocking: Boolean): Unit = {
        MethodUtils.invokeMethod(broadcast, true, "doUnpersist", Array(blocking))
      }
      override protected def doDestroy(blocking: Boolean): Unit = {
        MethodUtils.invokeMethod(broadcast, true, "doDestroy", Array(blocking))
      }
    }
    dummyBroadcasted.asInstanceOf[Broadcast[T]]
  }

  override def withNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
    copy(child = newChildren.head)
}
