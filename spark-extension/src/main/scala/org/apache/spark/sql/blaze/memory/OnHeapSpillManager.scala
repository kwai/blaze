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

      logInfo(
        "blaze allocated a heap-spill" +
          f", task=${taskContext.taskAttemptId}" +
          f", id=${spill.id}")
      spill.id
    }
  }

  def writeSpill(spillId: Int, data: ByteBuffer): Unit = {
    spills(spillId).get.write(data)

    // manually trigger spill if the amount of used memory exceeded limits
    val used = memUsed
    if (used > HEAP_SPILL_MEM_LIMIT) {
      spill(HEAP_SPILL_MEM_LIMIT - used, this)
    }
  }

  def readSpill(spillId: Int, buf: ByteBuffer): Int = {
    spills(spillId).get.read(buf)
  }

  def completeSpill(spillId: Int): Unit = {
    spills(spillId).get.complete()
  }

  def getSpillDiskUsage(spillId: Int): Long = {
    spills(spillId).get.diskUsed
  }

  def releaseSpill(spillId: Int): Unit = {
    spills(spillId).foreach(_.release())
    spills(spillId) = None
  }

  override def spill(size: Long, trigger: MemoryConsumer): Long = {
    logInfo(
      "OnHeapSpillManager starts spilling" +
        s", size=${Utils.bytesToString(size)}}" +
        s", currentUsed=${Utils.bytesToString(memUsed)}")
    var totalFreed = 0L

    Utils.tryWithSafeFinally {
      synchronized {
        while (totalFreed < size) {
          // find the max in-mem spill and spill it into disk file
          spills.maxBy(_.map(_.memUsed).getOrElse(-1L)) match {
            case Some(maxSpill) if maxSpill.memUsed > 0 =>
              val freed = maxSpill.spill()
              if (freed == 0) {
                return totalFreed
              }
              totalFreed += freed

            case _ =>
              return totalFreed // no spills can be freed
          }
        }
      }
    } {
      logInfo(s"OnHeapSpillManager finished spilling: freed=${Utils.bytesToString(totalFreed)}")
    }
    totalFreed
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
