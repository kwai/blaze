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

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.blaze.memory.OnHeapSpill.BLOCK_SIZE
import org.apache.spark.storage.BlockId
import org.apache.spark.util.Utils

case class OnHeapSpill(hsm: OnHeapSpillManager, id: Int) extends Logging {
  private var status: OnHeapSpill.Status = OnHeapSpill.Writing
  private var diskSpilledBlock: Option[(BlockId, RandomAccessFile, FileChannel)] = None
  private var diskSpilledReadOffset = 0L
  private var writtenSize = 0L
  private var readSize = 0L
  private val dataBlockQueue = mutable.Queue[ByteBuffer]()

  def memUsed: Long = dataBlockQueue.length * BLOCK_SIZE

  def diskUsed: Long =
    diskSpilledBlock match {
      case Some((_, file, _)) => file.length()
      case None => 0
    }

  def write(buf: ByteBuffer): Unit = {
    assert(status == OnHeapSpill.Writing)

    // allocate memory for incoming data
    val numBytesWritingToNewBlock =
      buf.remaining() - dataBlockQueue.lastOption.map(_.remaining()).getOrElse(0)
    val numNewBlocks = (numBytesWritingToNewBlock + BLOCK_SIZE - 1) / BLOCK_SIZE

    var ownedMemory: Long = dataBlockQueue.length * BLOCK_SIZE
    if (numNewBlocks > 0) {
      ownedMemory += hsm.acquireMemory(numNewBlocks * BLOCK_SIZE)
    }

    synchronized {
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

      // if not enough memory is allocated, spill all data in current heap-spill
      if (ownedMemory < memUsed) {
        spillAll()
        hsm.freeMemory(ownedMemory)
      }
    }
  }

  def read(buf: ByteBuffer): Int = {
    assert(status == OnHeapSpill.Reading)

    synchronized {
      diskSpilledBlock match {
        case Some((_, _, channel)) =>
          // read from file
          channel.position(readSize - diskSpilledReadOffset)
          while (buf.hasRemaining && channel.read(buf) >= 0) {}

        case None =>
          // read from in-mem data queue
          while (buf.hasRemaining && dataBlockQueue.nonEmpty) {
            if (!dataBlockQueue.head.hasRemaining) {
              hsm.freeMemory(dataBlockQueue.dequeue().capacity())
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
      }
      readSize += buf.position()
      buf.position()
    }
  }

  def complete(): Unit = {
    assert(status == OnHeapSpill.Writing)

    synchronized {
      // mark last block.hasRemaining = false
      dataBlockQueue.lastOption match {
        case Some(buf) if buf.hasRemaining => buf.limit(buf.position())
        case _ =>
      }

      if (this.diskSpilledBlock.nonEmpty) {
        val freed = this.spillInternal() // spill all data if it is partially spilled
        hsm.freeMemory(freed)
      }
      dataBlockQueue.foreach(_.flip())
      status = OnHeapSpill.Reading
    }

    logInfo(
      "completed on-heap spill data" +
        f", task=${TaskContext.get.taskAttemptId}" +
        f", id=$id" +
        f", size=${Utils.bytesToString(writtenSize)}")
  }

  def spill(): Long = {
    synchronized {
      val freed = spillInternal()
      hsm.freeMemory(freed)
      freed
    }
  }

  /**
   * spill all data block into file and returen freed memory size
   * @return freed memory size
   */
  private def spillInternal(): Long = {
    var freed = 0L
    val (_, file, channel) = diskSpilledBlock match {
      case Some((blockId, file, channel)) => (blockId, file, channel)
      case _ =>
        val (blockId, file) = hsm.blockManager.diskBlockManager.createTempLocalBlock()
        val randomAccessFile = new RandomAccessFile(file, "rw")
        val channel = randomAccessFile.getChannel
        diskSpilledBlock = Some((blockId, randomAccessFile, channel))
        diskSpilledReadOffset = readSize
        (blockId, randomAccessFile, channel)
    }
    channel.position(file.length())

    // write all completed blocks to file
    status match {
      case OnHeapSpill.Writing =>
        while (dataBlockQueue.nonEmpty && !dataBlockQueue.head.hasRemaining) {
          val buf = dataBlockQueue.dequeue()
          buf.flip()
          while (buf.hasRemaining && channel.write(buf) >= 0) {}
          freed += BLOCK_SIZE
        }

      case OnHeapSpill.Reading =>
        while (dataBlockQueue.nonEmpty) {
          val buf = dataBlockQueue.dequeue()
          while (buf.hasRemaining && channel.write(buf) >= 0) {}
          freed += BLOCK_SIZE
        }
    }

    logInfo(
      "on-heap spill data spilled into file" +
        f", task=${TaskContext.get.taskAttemptId}" +
        f", id=$id" +
        f", freed=${Utils.bytesToString(freed)}")
    freed
  }

  private def spillAll(): Long = {
    if (this.diskSpilledBlock.isEmpty) {
      return 0L
    }

    dataBlockQueue.lastOption match {
      case Some(buf) if buf.hasRemaining => buf.limit(buf.position())
      case _ =>
    }
    this.spillInternal() // spill all data if it is partially spilled
  }

  def release(): Unit = {
    this.synchronized {
      val freed = memUsed

      // release in-mem data
      dataBlockQueue.clear()
      hsm.freeMemory(freed)

      // release disk data
      diskSpilledBlock.foreach {
        case (blockId, _, channel) =>
          channel.close()
          hsm.blockManager.removeBlock(blockId, tellMaster = false)
      }
      diskSpilledBlock = None

      logInfo(
        "on-heap spill data released" +
          f", task=${TaskContext.get.taskAttemptId}" +
          f", id=$id" +
          f", freed=${Utils.bytesToString(freed)}")
    }
  }
}

object OnHeapSpill {
  val BLOCK_SIZE: Int = 1048576

  sealed abstract class Status()
  final case object Writing extends Status()
  final case object Reading extends Status()
}
