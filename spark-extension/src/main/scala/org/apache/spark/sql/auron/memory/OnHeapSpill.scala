/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.auron.memory

import java.nio.ByteBuffer

import org.apache.spark.internal.Logging

case class OnHeapSpill(hsm: OnHeapSpillManager, id: Int) extends Logging {
  private var spillBuf: SpillBuf = new MemBasedSpillBuf

  def memUsed: Long = spillBuf.memUsed
  def diskUsed: Long = spillBuf.diskUsed
  def size: Long = spillBuf.size
  def diskIOTime: Long = spillBuf.diskIOTime

  def write(buf: ByteBuffer): Unit = {
    var needSpill = false
    synchronized {
      spillBuf match {
        case _: MemBasedSpillBuf =>
          val acquiredMemory = hsm.acquireMemory(buf.capacity())
          if (acquiredMemory < buf.capacity()) { // cannot allocate memory, will spill buffer
            hsm.freeMemory(acquiredMemory)
            needSpill = true
          }
        case _ =>
      }
    }

    if (needSpill) {
      spillInternal()
    }

    synchronized {
      spillBuf.write(buf)
    }
  }

  def read(buf: ByteBuffer): Int = {
    synchronized {
      val oldMemUsed = memUsed
      val startPosition = buf.position()
      spillBuf.read(buf)

      val numBytesRead = buf.position() - startPosition

      // some memory can be released while reading
      val newMemUsed = memUsed
      if (newMemUsed < oldMemUsed) {
        hsm.freeMemory(oldMemUsed - newMemUsed)
      }
      numBytesRead
    }
  }

  def release(): Unit = {
    synchronized {
      val oldMemUsed = memUsed
      spillBuf = new ReleasedSpillBuf(spillBuf)

      if (oldMemUsed > 0) {
        hsm.freeMemory(oldMemUsed)
      }
    }
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
