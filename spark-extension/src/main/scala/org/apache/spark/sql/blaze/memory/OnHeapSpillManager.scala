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
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.EXECUTOR_MEMORY
import org.apache.spark.internal.config.MEMORY_FRACTION
import org.apache.spark.memory.MemoryConsumer
import org.apache.spark.storage.BlockManager
import org.apache.spark.util.Utils

class OnHeapSpillManager(taskContext: TaskContext)
    extends MemoryConsumer(taskContext.taskMemoryManager)
    with Logging {
  import org.apache.spark.sql.blaze.memory.OnHeapSpillManager._

  private val _blockManager = SparkEnv.get.blockManager
  private val spills = ArrayBuffer[Option[OnHeapSpill]]()
  private var numHoldingSpills = 0

  // release all spills on task completion
  taskContext.addTaskCompletionListener { _ =>
    val taskId = taskContext.taskAttemptId()
    spills.flatten.foreach(spill => releaseSpill(spill.id))
    all.remove(taskId)
  }

  def blockManager: BlockManager = _blockManager

  def memUsed: Long = getUsed

  /**
   * allocate a new spill and return its id
   * @return allocated spill id
   */
  def newSpill(): Int = {
    synchronized {
      val spill = OnHeapSpill(this, spills.length)
      spills.append(Some(spill))
      numHoldingSpills += 1

      logInfo(s"blaze allocated a heap-spill, task=${taskContext.taskAttemptId}, id=${spill.id}")
      dumpStatus()
      spill.id
    }
  }

  def writeSpill(spillId: Int, data: ByteBuffer): Unit = {
    spills(spillId).get.write(data)

    // manually trigger spill if the amount of used memory exceeded limits
    val totalUsed = all.values.map(_.memUsed).sum
    if (totalUsed > HEAP_SPILL_MEM_LIMIT) {
      all.values.maxBy(_.memUsed).spill(totalUsed - HEAP_SPILL_MEM_LIMIT, this)
    }
  }

  def readSpill(spillId: Int, buf: ByteBuffer): Int = {
    spills(spillId).get.read(buf)
  }

  def completeSpill(spillId: Int): Unit = {
    val spill = spills(spillId).get
    spill.complete()

    logInfo(
      "blaze completed a heap-spill" +
        s", task=${taskContext.taskAttemptId}" +
        s", id=$spillId" +
        s", size=${Utils.bytesToString(spill.size)}")
    dumpStatus()
  }

  def getSpillDiskUsage(spillId: Int): Long = {
    spills(spillId).map(_.diskUsed).getOrElse(0)
  }

  def releaseSpill(spillId: Int): Unit = {
    spills(spillId) match {
      case Some(spill) =>
        spill.release()
        numHoldingSpills -= 1
        logInfo(s"blaze released a heap-spill, task: ${taskContext.taskAttemptId}, id: $spillId")
        dumpStatus()
      case None =>
    }
    spills(spillId) = None
  }

  override def spill(size: Long, trigger: MemoryConsumer): Long = {
    logInfo(s"OnHeapSpillManager starts spilling, size=${Utils.bytesToString(size)}}")
    dumpStatus()
    var totalFreed = 0L

    Utils.tryWithSafeFinally {
      synchronized {
        spills.foreach {
          case Some(spill) if spill.memUsed > 0 =>
            totalFreed += spill.spill()
            if (totalFreed >= size) {
              return totalFreed
            }
          case _ =>
        }
      }
    } {
      logInfo(s"OnHeapSpillManager finished spilling: freed=${Utils.bytesToString(totalFreed)}")
      dumpStatus()
    }
    totalFreed
  }

  private def dumpStatus(): Unit = {
    logInfo(
      s"OnHeapSpillManager (task=${taskContext.taskAttemptId}) status" +
        s": numHoldingSpills=$numHoldingSpills" +
        s", memUsed=${Utils.bytesToString(memUsed)}")
  }
}

object OnHeapSpillManager extends Logging {
  val HEAP_SPILL_MEM_LIMIT: Long = {
    val conf = SparkEnv.get.conf
    val memoryFraction = conf.get(MEMORY_FRACTION)
    val heapSpillFraction = conf.getDouble("spark.blaze.heap.spill.fraction", 0.8)
    val executorMemory = conf.get(EXECUTOR_MEMORY) * 1024 * 1024
    val heapSpillMemLimit = (executorMemory * memoryFraction * heapSpillFraction).toLong
    logInfo(s"heapSpillMemLimit=${Utils.bytesToString(heapSpillMemLimit)}")
    heapSpillMemLimit
  }
  val all: mutable.Map[Long, OnHeapSpillManager] = mutable.Map()

  def current: OnHeapSpillManager = {
    val taskContext = TaskContext.get
    all.getOrElseUpdate(taskContext.taskAttemptId(), new OnHeapSpillManager(taskContext))
  }
}
