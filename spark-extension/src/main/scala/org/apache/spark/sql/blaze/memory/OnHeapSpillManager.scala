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

package org.apache.spark.sql.blaze.memory

import java.nio.ByteBuffer

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkEnv
import org.apache.spark.TaskContext

import org.apache.spark.internal.config.EXECUTOR_MEMORY
import org.apache.spark.internal.Logging
import org.apache.spark.memory.MemoryConsumer
import org.apache.spark.util.Utils

class OnHeapSpillManager(taskContext: TaskContext)
    extends MemoryConsumer(taskContext.taskMemoryManager)
    with Logging {
  import org.apache.spark.sql.blaze.memory.OnHeapSpillManager._
  private val spills = ArrayBuffer[Option[OnHeapSpill]]()

  // release all spills on task completion
  taskContext.addTaskCompletionListener { _ =>
    val taskId = taskContext.taskAttemptId()
    spills.flatten.foreach(spill => releaseSpill(spill.id))
    all.remove(taskId)
  }

  /**
   * allocate a new spill and return its id, returns -1 if allocation failed
   * @param capacity spill bytes size
   * @return allocated spill id, or -1
   */
  def newSpill(capacity: Long): Int = {
    var totalUsed = all.values.map(_.getUsed).sum
    if (totalUsed + capacity > HEAP_SPILL_MEM_LIMIT) {
      logWarning(
        "cannot allocate a new heap-spill" +
          f", acquiring=${Utils.bytesToString(capacity)}" +
          f", totalUsed=${Utils.bytesToString(totalUsed)}")
      return -1
    }
    val acquired = this.acquireMemory(capacity)
    if (acquired < capacity) {
      logWarning(
        "cannot allocate a new heap-spill" +
          f", acquiring=${Utils.bytesToString(capacity)}" +
          f", acquired=${Utils.bytesToString(acquired)}")
      this.freeMemory(acquired)
      return -1
    }

    val taskId = TaskContext.get.taskAttemptId()
    val spillId = spills.length
    spills.append(Some(OnHeapSpill(spillId, capacity)))
    totalUsed += capacity
    logInfo(
      "blaze allocated a heap-spill" +
        f", taskId=$taskId" +
        f", spillId=$spillId" +
        f", acquired=${Utils.bytesToString(capacity)}" +
        f", totalUsed=${Utils.bytesToString(totalUsed)}")
    spillId
  }

  def writeSpill(spillId: Int, data: ByteBuffer): Unit = {
    spills(spillId).get.write(data)
  }

  def readSpill(spillId: Int, buf: ByteBuffer): Int = {
    spills(spillId).get.read(buf)
  }

  def completeSpill(spillId: Int): Unit = {
    spills(spillId).get.complete()
  }

  def releaseSpill(spillId: Int): Unit = {
    spills(spillId).foreach(spill => this.freeMemory(spill.capacity))
    spills(spillId) = None
  }

  // we do not support spilling a heap-spill at the moment
  override def spill(size: Long, trigger: MemoryConsumer): Long = 0
}

object OnHeapSpillManager extends Logging {
  val HEAP_SPILL_MEM_LIMIT: Long = {
    val conf = SparkEnv.get.conf
    val fraction = conf.getDouble("blaze.heap.spill.fraction", 0.50)
    val executorMemory = conf.get(EXECUTOR_MEMORY) * 1024 * 1024
    val heapSpillMemLimit = (executorMemory * fraction).toLong
    logInfo(s"heapSpillMemLimit=${Utils.bytesToString(heapSpillMemLimit)}")
    heapSpillMemLimit
  }
  val all: mutable.Map[Long, OnHeapSpillManager] = mutable.Map()

  def current: OnHeapSpillManager = {
    val taskContext = TaskContext.get
    all.getOrElseUpdate(taskContext.taskAttemptId(), new OnHeapSpillManager(taskContext))
  }
}
