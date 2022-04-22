package org.apache.spark.sql.blaze.execution

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.ByteOrder

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.FileSegmentManagedBuffer
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.buffer.NettyManagedBuffer
import org.apache.spark.network.buffer.NioManagedBuffer
import org.apache.spark.sql.catalyst.InternalRow
import org.blaze.FileSegmentSeekableByteChannel
import org.blaze.NioSeekableByteChannel

object Converters extends Logging {

  /**
   * Parse ManagedBuffer from shuffle reader into record iterator.
   * Each ManagedBuffer may be composed of one or more IPC entities.
   */
  def readManagedBuffer(data: ManagedBuffer, context: TaskContext): Iterator[InternalRow] = {
    val segmentSeekableByteChannels = readManagedBufferToSegmentByteChannels(data)
    segmentSeekableByteChannels.toIterator.flatMap(channel =>
      new ArrowReaderIterator(channel, context).result)
  }

  def readManagedBufferToSegmentByteChannels(data: ManagedBuffer): Seq[SeekableByteChannel] = {
    val result: ArrayBuffer[SeekableByteChannel] = ArrayBuffer()
    data match {
      case f: FileSegmentManagedBuffer =>
        val file = f.getFile
        val lengthReader = new RandomAccessFile(file, "r")
        var curEnd = f.getOffset + f.getLength

        while (curEnd > f.getOffset) {
          val lenBuf = ByteBuffer.allocate(8)
          lengthReader.seek(curEnd - 8)
          lengthReader.read(lenBuf.array())
          val len = lenBuf.order(ByteOrder.LITTLE_ENDIAN).getLong(0).toInt
          val curStart = curEnd - 8 - len
          val fsc = new FileSegmentSeekableByteChannel(file, curStart, len)
          result += fsc
          curEnd = curStart
        }

      case _: NettyManagedBuffer | _: NioManagedBuffer =>
        val all = data.nioByteBuffer()
        var curEnd = all.limit()

        while (curEnd > 0) {
          val lenBuf = ByteBuffer.allocate(8)
          all.position(curEnd - 8)
          lenBuf.putLong(all.getLong)
          val len = lenBuf.order(ByteOrder.LITTLE_ENDIAN).getLong(0).toInt
          val curStart = curEnd - 8 - len
          val sc = new NioSeekableByteChannel(all, curStart, len)
          result += sc
          curEnd = curStart
        }
      case mb =>
        throw new UnsupportedOperationException(s"ManagedBuffer of $mb not supported")
    }
    result
  }
}
