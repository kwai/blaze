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

import java.io.IOException
import java.io.ObjectInputStream
import java.io.ObjectOutputStream

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.Dependency
import org.apache.spark.Partition
import org.apache.spark.Partitioner
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.blaze.protobuf.PhysicalPlanNode

class NativeRDD(
    @transient private val rddSparkContext: SparkContext,
    val metrics: MetricNode,
    private val rddPartitions: Array[Partition],
    private val rddPartitioner: Option[Partitioner],
    private val rddDependencies: Seq[Dependency[_]],
    private val rddShuffleReadFull: Boolean,
    @transient private val nativePlan: (Partition, TaskContext) => PhysicalPlanNode,
    val friendlyName: String = null)
    extends RDD[InternalRow](rddSparkContext, rddDependencies)
    with Logging
    with Serializable {

  // use serializable wrapper to avoid serializing nativePlan
  val nativePlanWrapper = new NativePlanWrapper(nativePlan)

  if (friendlyName != null) {
    setName(friendlyName)
  }

  def nativePlan(p: Partition, tc: TaskContext): PhysicalPlanNode = {
    nativePlanWrapper.plan(p, tc)
  }

  def isShuffleReadFull: Boolean = Shims.get.getRDDShuffleReadFull(this)
  Shims.get.setRDDShuffleReadFull(this, rddShuffleReadFull)

  override protected def getPartitions: Array[Partition] = rddPartitions
  override protected def getDependencies: Seq[Dependency[_]] = rddDependencies
  override val partitioner: Option[Partitioner] = rddPartitioner

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val computingNativePlan = nativePlanWrapper.plan(split, context)
    NativeHelper.executeNativePlan(computingNativePlan, metrics, split, Some(context))
  }
}

class NativePlanWrapper(var p: (Partition, TaskContext) => PhysicalPlanNode)
    extends Serializable {
  def plan(split: Partition, context: TaskContext): PhysicalPlanNode = {
    p(split, context)
  }

  @throws[IOException]
  private def writeObject(out: ObjectOutputStream): Unit = {
    out.writeObject(p)
  }

  @throws[IOException]
  @throws[ClassNotFoundException]
  private def readObject(in: ObjectInputStream): Unit = {
    NativePlanWrapper.changeProtobufDefaultRecursionLimit
    p = in.readObject.asInstanceOf[(Partition, TaskContext) => PhysicalPlanNode]
  }
}

object NativePlanWrapper extends Logging {

  // change protobuf's default recursion limit to Int.MAX_VALUE to walk-around
  // `Protocol message had too many levels of nesting` error.
  private lazy val changeProtobufDefaultRecursionLimit: Unit = {
    try {
      val recursionLimitField =
        classOf[com.google.protobuf.CodedInputStream].getDeclaredField("defaultRecursionLimit")
      recursionLimitField.setAccessible(true)
      recursionLimitField.setInt(null, Int.MaxValue)
    } catch {
      case e: Throwable =>
        logWarning("error changing protobuf's default recursion limit to Int.MaxValue", e)
    }
  }
}
