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

import org.apache.spark.internal.Logging

case class OnHeapSpill(hsm: OnHeapSpillManager, id: Int) extends Logging {
  private var spillBuf: SpillBuf = new MemBasedSpillBuf

  def memUsed: Long = spillBuf.memUsed
  def diskUsed: Long = spillBuf.diskUsed
  def size: Long = spillBuf.size
  def diskIOTime: Long =
    spillBuf match {
      case _: MemBasedSpillBuf => 0
      case fileBasedBuf: FileBasedSpillBuf => fileBasedBuf.diskIOTimeNs
    }

  def write(buf: ByteBuffer): Unit = {
    synchronized {
      spillBuf match {
        case _: MemBasedSpillBuf =>
          val acquiredMemory = hsm.acquireMemory(buf.capacity())
          if (acquiredMemory < buf.capacity()) { // cannot allocate memory, will spill buffer
            hsm.freeMemory(acquiredMemory)
            spillInternal()
          }
        case _ =>
      }
      spillBuf.write(buf)
    }
  }

  def read(buf: ByteBuffer): Int = {
    synchronized {
      val startPosition = buf.position()
      spillBuf.read(buf)
      buf.position() - startPosition
    }
  }

  def release(): Unit = {
    spillBuf.release()
    spillBuf = null
  }

  def spill(): Long = {
    synchronized {
      spillInternal()
    }
  }

  private def spillInternal(): Long = {
    spillBuf match {
      case memBasedBuf: MemBasedSpillBuf =>
        val releasingMemory = memUsed
        spillBuf = memBasedBuf.spill(hsm)
        hsm.freeMemory(releasingMemory)
        releasingMemory
    }
  }
}
