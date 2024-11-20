package org.apache.spark.blaze

import java.io.EOFException
import java.nio.ByteBuffer
import java.nio.channels.Channels

import org.apache.hadoop.fs.FSDataOutputStream

trait FSDataOutputWrapper extends AutoCloseable {
  def writeFully(buf: ByteBuffer): Unit
}

object FSDataOutputWrapper {
  def wrap(output: FSDataOutputStream): FSDataOutputWrapper = {
    new SeekableFSDataOutputWrapper(output)
  }
}

class SeekableFSDataOutputWrapper(output: FSDataOutputStream) extends FSDataOutputWrapper {
  override def writeFully(buf: ByteBuffer): Unit = {
    output.synchronized {
      val channel = Channels.newChannel(output)
      while (buf.hasRemaining) if (channel.write(buf) == -1) {
        throw new EOFException("writeFullyToFSDataOutputStream() got unexpected EOF")
      }
    }
  }

  override def close(): Unit = output.close()
}