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

import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.util

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

abstract class SpillBuf {
  def write(buf: ByteBuffer): Unit
  def read(buf: ByteBuffer): Unit
  def release(): Unit
  def memUsed: Long
  def diskUsed: Long
  def diskIOTime: Long
  def size: Long
}

class MemBasedSpillBuf extends SpillBuf with Logging {
  private val bufs = new util.ArrayDeque[ByteBuf]()
  private var numWrittenBytes: Long = 0
  private var mem: Long = 0

  override def write(buf: ByteBuffer): Unit = {
    if (buf.isDirect) {
      val numBytes = buf.limit()
      val copiedBuf = Unpooled.copiedBuffer(buf)
      numWrittenBytes += numBytes
      mem += numBytes
      bufs.addLast(copiedBuf)
    } else {
      val numBytes = buf.capacity()
      numWrittenBytes += numBytes
      mem += numBytes
      bufs.addLast(Unpooled.wrappedBuffer(buf))
    }
  }

  override def read(buf: ByteBuffer): Unit = {
    while (buf.hasRemaining && !bufs.isEmpty) {
      val readLen = buf.remaining().min(bufs.peekFirst.readableBytes())
      val dup = buf.duplicate()
      dup.limit(dup.position() + readLen)
      bufs.peekFirst.readBytes(dup)
      buf.position(buf.position() + readLen)

      if (bufs.peekFirst.readableBytes() == 0) {
        val popped = bufs.removeFirst()
        mem -= popped.capacity()
      }
    }
  }

  override def release(): Unit = {
    bufs.clear()
    mem = 0
  }

  override def memUsed: Long = mem
  override def diskUsed: Long = 0
  override def diskIOTime: Long = 0
  override def size: Long = numWrittenBytes

  def spill(hsm: OnHeapSpillManager): FileBasedSpillBuf = {
    logWarning(s"spilling in-mem spill buffer to disk, size=${Utils.bytesToString(size)}")

    val startTimeNs = System.nanoTime()
    val file = hsm.blockManager.diskBlockManager.createTempLocalBlock()._2
    val channel = new RandomAccessFile(file, "rw").getChannel

    while (!bufs.isEmpty) {
      val buf = bufs.removeFirst().nioBuffer()
      while (buf.remaining() > 0) {
        channel.write(buf)
      }
    }
    val endTimeNs = System.nanoTime
    new FileBasedSpillBuf(numWrittenBytes, file, channel, endTimeNs - startTimeNs)
  }
}

class FileBasedSpillBuf(
    numWrittenBytes: Long,
    file: File,
    fileChannel: FileChannel,
    var diskIOTimeNs: Long)
    extends SpillBuf
    with Logging {

  private var readPosition: Long = 0
  private var closedFileDiskUsage: Long = 0

  override def write(buf: ByteBuffer): Unit = {
    val startTimeNs = System.nanoTime()
    while (buf.hasRemaining) {
      fileChannel.write(buf)
    }
    diskIOTimeNs += System.nanoTime() - startTimeNs
  }

  override def read(buf: ByteBuffer): Unit = {
    val startTimeNs = System.nanoTime()
    while (buf.hasRemaining && readPosition < fileChannel.size()) {
      readPosition += fileChannel.read(buf, readPosition)
    }
    diskIOTimeNs += System.nanoTime() - startTimeNs
  }

  override val memUsed: Long = 0
  override def diskUsed: Long = {
    if (fileChannel.isOpen) {
      fileChannel.size()
    } else {
      closedFileDiskUsage
    }
  }
  override def diskIOTime: Long = diskIOTimeNs
  override def size: Long = numWrittenBytes

  override def release(): Unit = {
    if (fileChannel.isOpen) {
      closedFileDiskUsage = fileChannel.size()
      fileChannel.close()
    }
    if (!file.delete()) {
      logWarning(s"Was unable to delete spill file: ${file.getAbsolutePath}")
    }
  }
}

class ReleasedSpillBuf(releasing: SpillBuf) extends SpillBuf {
  override val memUsed: Long = releasing.memUsed
  override val diskUsed: Long = releasing.diskUsed
  override val diskIOTime: Long = releasing.diskIOTime
  override val size: Long = releasing.size

  releasing.release()

  override def write(buf: ByteBuffer): Unit =
    throw new UnsupportedOperationException()

  override def read(buf: ByteBuffer): Unit =
    throw new UnsupportedOperationException()

  override def release(): Unit = {}
}
