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

package org.apache.spark.sql.blaze.execution

import java.io.BufferedOutputStream
import java.io.File
import java.io.FileOutputStream
import java.io.OutputStream
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.channels.ClosedByInterruptException
import java.nio.ByteOrder

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.commons.io.output.CountingOutputStream
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util2.ArrowUtils2
import org.apache.spark.sql.util2.ArrowWriter
import org.apache.spark.storage.FileSegment
import org.apache.spark.storage.TimeTrackingOutputStream
import org.apache.spark.util.Utils
import org.apache.spark.SparkEnv

/**
 * A class for writing JVM objects directly to a file on disk. This class allows data to be appended
 * to an existing block. For efficiency, it retains the underlying file channel across
 * multiple commits. This channel is kept open until close() is called. In case of faults,
 * callers should instead close with revertPartialWritesAndClose() to atomically revert the
 * uncommitted partial writes.
 *
 * This class does not support concurrent writes. Also, once the writer has been opened it cannot be
 * reopened again.
 */
private[spark] class DiskBlockArrowIPCWriter(
    val file: File,
    bufferSize: Int,
    syncWrites: Boolean,
    // These write metrics concurrently shared with other active DiskBlockObjectWriters who
    // are themselves performing writes. All updates must be relative.
    writeMetrics: ShuffleWriteMetricsReporter,
    schema: StructType,
    maxRecordsPerBatch: Int)
    extends OutputStream
    with Logging {

  private val timezoneId: String = SparkEnv.get.conf.get(SQLConf.SESSION_LOCAL_TIMEZONE)
  private val arrowSchema: Schema = ArrowUtils2.toArrowSchema(schema, timezoneId)
  private val allocator: BufferAllocator =
    ArrowUtils2.rootAllocator.newChildAllocator("row2ArrowBatchWrite", 0, Long.MaxValue)
  private val root: VectorSchemaRoot = VectorSchemaRoot.create(arrowSchema, allocator)
  private val arrowBuffer: ArrowWriter = ArrowWriter.create(root)
  private val ipcLengthBuffer = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN)

  /** The file channel, used for repositioning / truncating the file. */
  private var randomAccessFile: RandomAccessFile = _
  private var countingUncompressed: CountingOutputStream = _
  private var mcs: ManualCloseBufferedOutputStream = _

  private var initialized = false
  private var streamOpen = false
  private var hasBeenClosed = false
  private var partitionStart: Long = _

  /**
   * Cursors used to represent positions in the file.
   *
   * xxxxxxxxxx|----------|-----|
   * ^          ^     ^
   * |          |    channel.position()
   * |        reportedPosition
   * committedPosition
   *
   * reportedPosition: Position at the time of the last update to the write metrics.
   * committedPosition: Offset after last committed write.
   * -----: Current writes to the underlying file.
   * xxxxx: Committed contents of the file.
   */
  private var committedPosition = file.length()
  private var reportedPosition = committedPosition

  /**
   * Keep track of number of records written and also use this to periodically
   * output bytes written since the latter is expensive to do for each record.
   * And we reset it after every commitAndGet called.
   */
  private var numRecordsWritten = 0
  private var currentRowCount = 0
  private var currentPartitionRowCount = 0
  private var writer: ArrowStreamWriter = _

  /**
   * Commits any remaining partial writes and closes resources.
   */
  override def close(): Unit = {
    if (initialized) {
      Utils.tryWithSafeFinally {
        commitAndGet()
        root.close()
        allocator.close()
      } {
        closeResources()
      }
    }
  }

  /**
   * Close and cleanup all resources.
   * Should call after committing or reverting partial writes.
   */
  private def closeResources(): Unit = {
    if (initialized) {
      Utils.tryWithSafeFinally {
        randomAccessFile.close()
      } {
        randomAccessFile = null
        mcs = null
        countingUncompressed = null
        initialized = false
        streamOpen = false
        hasBeenClosed = true
      }
    }
  }

  /**
   * Flush the partial writes and commit them as a single atomic block.
   * A commit may write additional bytes to frame the atomic block.
   *
   * @return file segment with previous offset and length committed on this call.
   */
  def commitAndGet(): FileSegment = {
    if (streamOpen) {
      // NOTE: Because Kryo doesn't flush the underlying stream we explicitly flush both the
      //       serializer stream and the lower level stream.
      streamOpen = false
      endPartition()
      updateBytesWritten()

      if (syncWrites) {
        // Force outstanding writes to disk and track how long it takes
        val start = System.nanoTime()
        randomAccessFile.getFD.sync()
        writeMetrics.incWriteTime(System.nanoTime() - start)
      }

      val pos = randomAccessFile.length()
      val fileSegment = new FileSegment(file, committedPosition, pos - committedPosition)
      committedPosition = pos
      // In certain compression codecs, more bytes are written after streams are closed
      writeMetrics.incBytesWritten(committedPosition - reportedPosition)
      reportedPosition = committedPosition
      numRecordsWritten = 0
      fileSegment
    } else {
      new FileSegment(file, committedPosition, 0)
    }
  }

  private def endPartition(): Unit = {
    Utils.tryWithSafeFinally {
      if (currentPartitionRowCount > 0) {
        if (currentRowCount > 0) { // finish current arrow data
          arrowBuffer.finish()
          writer.writeBatch()
          arrowBuffer.reset()
          currentRowCount = 0
        }

        // write compressed arrow data to file
        writer.end()
        writer.close()
        writer = null
        mcs.flush()

        // append length
        val current = randomAccessFile.length()
        val ipcLength = current - partitionStart - ipcLengthBuffer.capacity()
        val ipcLengthUncompressed = countingUncompressed.getByteCount
        ipcLengthBuffer.clear()
        ipcLengthBuffer.putLong(ipcLength)
        ipcLengthBuffer.putLong(ipcLengthUncompressed)
        ipcLengthBuffer.flip()
        randomAccessFile.seek(partitionStart)
        randomAccessFile.write(ipcLengthBuffer.array())
        randomAccessFile.seek(current)
        currentPartitionRowCount = 0
      }
    } {
      if (writer != null) {
        writer.close()
        writer = null
      }
    }
  }

  /**
   * Reverts writes that haven't been committed yet. Callers should invoke this function
   * when there are runtime exceptions. This method will not throw, though it may be
   * unsuccessful in truncating written data.
   *
   * @return the file that this DiskBlockObjectWriter wrote to.
   */
  def revertPartialWritesAndClose(): File = {
    // Discard current writes. We do this by flushing the outstanding writes and then
    // truncating the file to its initial position.
    Utils.tryWithSafeFinally {
      if (initialized) {
        writeMetrics.decBytesWritten(reportedPosition - committedPosition)
        writeMetrics.decRecordsWritten(numRecordsWritten)
        streamOpen = false
        closeResources()
      }
    } {
      var truncateStream: FileOutputStream = null
      try {
        truncateStream = new FileOutputStream(file, true)
        truncateStream.getChannel.truncate(committedPosition)
      } catch {
        // ClosedByInterruptException is an excepted exception when kill task,
        // don't log the exception stack trace to avoid confusing users.
        // See: SPARK-28340
        case ce: ClosedByInterruptException =>
          logError(
            s"Exception occurred while reverting partial writes to file $file}, ${ce.getMessage}")
        case e: Exception =>
          logError(s"Uncaught exception while reverting partial writes to file $file", e)
      } finally {
        if (truncateStream != null) {
          truncateStream.close()
          truncateStream = null
        }
      }
    }
    file
  }

  override def write(b: Int): Unit = throw new UnsupportedOperationException()

  override def write(kvBytes: Array[Byte], offs: Int, len: Int): Unit =
    throw new UnsupportedOperationException()

  def write(row: InternalRow): Unit = {
    if (!streamOpen) {
      open()
    }

    // setup arrow writer only when partition is not empty
    if (currentPartitionRowCount == 0) {
      ipcLengthBuffer.clear()
      mcs.write(ipcLengthBuffer.array()) // ipc length placeholder
      countingUncompressed = new CountingOutputStream(
        ArrowShuffleManager301.compressionCodecForShuffling.compressedOutputStream(mcs))

      val channel = Channels.newChannel(countingUncompressed)
      writer = new ArrowStreamWriter(root, new MapDictionaryProvider(), channel)
      writer.start()
    }

    currentRowCount += 1
    currentPartitionRowCount += 1
    arrowBuffer.write(row)

    if (currentRowCount == maxRecordsPerBatch) {
      arrowBuffer.finish()
      writer.writeBatch()
      arrowBuffer.reset()
      currentRowCount = 0
    }
  }

  private def open(): DiskBlockArrowIPCWriter = {
    if (hasBeenClosed) {
      throw new IllegalStateException("Writer already closed. Cannot be reopened.")
    }
    if (!initialized) {
      initialize()
      initialized = true
    }

    streamOpen = true
    startPartition()
    this
  }

  private def initialize(): Unit = {
    randomAccessFile = new RandomAccessFile(file, "rw")

    val os = Channels.newOutputStream(randomAccessFile.getChannel)
    mcs = new ManualCloseBufferedOutputStream(new TimeTrackingOutputStream(writeMetrics, os))
  }

  private def startPartition(): Unit = {
    partitionStart = randomAccessFile.length()
    currentRowCount = 0
    currentPartitionRowCount = 0
  }

  /**
   * Notify the writer that a record worth of bytes has been written with OutputStream#write.
   */
  def recordWritten(): Unit = {
    numRecordsWritten += 1
    writeMetrics.incRecordsWritten(1)
  }

  /**
   * Report the number of bytes written in this writer's shuffle write metrics.
   * Note that this is only valid before the underlying streams are closed.
   */
  private def updateBytesWritten(): Unit = {
    val pos = randomAccessFile.length()
    writeMetrics.incBytesWritten(pos - reportedPosition)
    reportedPosition = pos
  }

  /**
   * Guards against close calls, e.g. from a wrapping stream.
   * Call manualClose to close the stream that was extended by this trait.
   * Commit uses this trait to close object streams without paying the
   * cost of closing and opening the underlying file.
   */
  private class ManualCloseBufferedOutputStream(inner: OutputStream)
      extends BufferedOutputStream(inner) {

    override def close(): Unit = {
      inner.flush()
    }

    def manualClose(): Unit = {
      inner.close()
    }
  }
}
