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

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

import scala.collection.mutable

import org.apache.spark.SparkException

import org.apache.spark.internal.Logging
import org.apache.spark.sql.blaze.memory.OnHeapSpill.BLOCK_SIZE

case class OnHeapSpill(hsm: OnHeapSpillManager, id: Int) extends Logging {
  private var status: OnHeapSpill.Status = OnHeapSpill.Writing
  private var diskSpilledFile: Option[FileChannel] = None
  private var diskSpilledReadOffset = 0L
  private var writtenSize = 0L
  private var readSize = 0L
  private val dataBlockQueue = mutable.Queue[ByteBuffer]()
  private var memAllocated = 0L

  def size: Long = writtenSize

  def memUsed: Long = dataBlockQueue.length * BLOCK_SIZE

  def diskUsed: Long =
    diskSpilledFile match {
      case Some(channel) => channel.size()
      case None => 0
    }

  def write(buf: ByteBuffer): Unit = {
    // reserve memory before writing
    val numLastBlockRestBytes = dataBlockQueue.lastOption
      .map(BLOCK_SIZE - _.position())
      .getOrElse(0)
    val numAcquiringBlocks = (buf.limit() + BLOCK_SIZE - 1 - numLastBlockRestBytes) / BLOCK_SIZE
    if (numAcquiringBlocks > 0) {
      if (diskSpilledFile.isEmpty && !acquireMemory(numAcquiringBlocks * BLOCK_SIZE)) {
        spill()
      }
    }

    synchronized {
      assert(status == OnHeapSpill.Writing)
      diskSpilledFile match {
        case Some(channel) =>
          // write to file
          channel.position(channel.size())
          while (buf.hasRemaining && channel.write(buf) >= 0) {}

        case None =>
          // write to in-mem data queue
          while (buf.hasRemaining) {
            if (!dataBlockQueue.lastOption.exists(_.hasRemaining)) {
              dataBlockQueue.enqueue(ByteBuffer.allocate(BLOCK_SIZE))
            }
            val destBuf = dataBlockQueue.last
            val srcBuf = buf.slice()
            val nTransfer = Math.min(srcBuf.remaining(), destBuf.remaining())
            srcBuf.limit(nTransfer)
            destBuf.put(srcBuf)
            buf.position(buf.position() + nTransfer)
            writtenSize += nTransfer
          }
          shrinkMemory()
      }
    }

  }

  def read(buf: ByteBuffer): Int = {
    synchronized {
      assert(status == OnHeapSpill.Reading)
      diskSpilledFile match {
        case Some(channel) =>
          // read from file
          channel.position(readSize - diskSpilledReadOffset)
          while (buf.hasRemaining && channel.read(buf) >= 0) {}

        case None =>
          // read from in-mem data queue
          while (buf.hasRemaining && dataBlockQueue.nonEmpty) {
            if (!dataBlockQueue.head.hasRemaining) {
              dataBlockQueue.dequeue()
            }
            if (dataBlockQueue.nonEmpty) {
              val srcBuf = dataBlockQueue.head
              val nTransfer = Math.min(srcBuf.remaining(), buf.remaining())

              val srcBufDuplicated = srcBuf.slice()
              srcBufDuplicated.limit(nTransfer)
              buf.put(srcBufDuplicated)

              srcBuf.position(srcBuf.position() + nTransfer)
            }
          }
          shrinkMemory()
      }
      readSize += buf.position()
      buf.position()
    }
  }

  def complete(): Unit = {
    synchronized {
      assert(status == OnHeapSpill.Writing)
      dataBlockQueue.foreach(_.flip())
      status = OnHeapSpill.Reading
      shrinkMemory(shrinkToFit = true)
    }
  }

  def spill(): Long = {
    synchronized {
      if (diskSpilledFile.nonEmpty) { // already spilled
        return 0L
      }
      spillInternal()
    }
  }

  /**
   * allocate memory from hsm
   * @param size the amount of allocating memory
   * @return true if the memory is fulfilled, otherwise false
   */
  private def acquireMemory(size: Long): Boolean = {
    val allocated = hsm.acquireMemory(size)
    if (allocated < size) {
      hsm.freeMemory(allocated)
      return false
    }
    memAllocated += allocated
    true
  }

  /**
   * shrink to actual memory usage
   */
  private def shrinkMemory(shrinkToFit: Boolean = false): Unit = {
    if (memAllocated > memUsed) {
      if (shrinkToFit || memAllocated - memUsed >= BLOCK_SIZE) {
        freeMemory(memAllocated - memUsed)
      }
    }
  }

  private def freeMemory(size: Long): Long = {
    val freed = Math.min(memAllocated, size)
    hsm.freeMemory(freed)
    memAllocated -= freed
    freed
  }

  /**
   * spill all data block into file and returen freed memory size
   * @return freed memory size
   */
  private def spillInternal(): Long = {
    val channel = diskSpilledFile match {
      case Some(_) => throw new RuntimeException("unreachable: double spilled")
      case _ =>
        val file = hsm.blockManager.diskBlockManager.createTempLocalBlock()._2
        val channel = new RandomAccessFile(file, "rw").getChannel
        diskSpilledFile = Some(channel)
        diskSpilledReadOffset = readSize
        channel
    }
    channel.position(channel.size())

    // flip for reading
    if (status == OnHeapSpill.Writing) {
      dataBlockQueue.foreach(_.flip())
    }

    // write all blocks to file
    while (dataBlockQueue.nonEmpty) {
      val buf = dataBlockQueue.dequeue()
      while (buf.hasRemaining && channel.write(buf) >= 0) {}
    }
    channel.force(false)
    freeMemory(memAllocated)
  }

  def release(): Unit = {
    this.synchronized {
      dataBlockQueue.clear()
      freeMemory(memAllocated)

      // release disk data
      diskSpilledFile.foreach(_.close())
      diskSpilledFile = None
    }
  }
}

object OnHeapSpill {
  val BLOCK_SIZE: Int = 1048576

  sealed abstract class Status()
  final case object Writing extends Status()
  final case object Reading extends Status()
}
