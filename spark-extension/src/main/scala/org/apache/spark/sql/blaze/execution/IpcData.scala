package org.apache.spark.sql.blaze.execution

import java.nio.channels.Channels
import java.nio.channels.ReadableByteChannel

import org.apache.spark.internal.Logging
import org.apache.spark.network.util.LimitedInputStream

case class IpcData(
  channel: ReadableByteChannel,
  compressed: Boolean,
  ipcLength: Long,
  ipcLengthUncompressed: Long)
  extends Logging {

  def readArrowData: ReadableByteChannel = {
    val is = new LimitedInputStream(Channels.newInputStream(channel), ipcLength, false)

    if (!compressed) {
      Channels.newChannel(is)
    } else {
      val zs = ArrowShuffleManager301.compressionCodecForShuffling.compressedInputStream(is)
      Channels.newChannel(zs)
    }
  }
}
