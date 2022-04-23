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

package org.apache.spark.sql.blaze.execution

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider
import org.apache.arrow.vector.ipc.ArrowFileWriter
import org.apache.arrow.vector.ipc.message.MessageSerializer
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.util2.ArrowWriter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util2.ArrowUtils2
import org.apache.spark.storage.{FileSegment, TimeTrackingOutputStream}
import org.apache.spark.util.Utils

import java.io.{BufferedOutputStream, File, FileOutputStream, OutputStream}
import java.nio.ByteBuffer
import java.nio.channels.{ClosedByInterruptException, FileChannel}
import org.apache.spark.sql.internal.SQLConf
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

  val timezoneId = SparkEnv.get.conf.get(SQLConf.SESSION_LOCAL_TIMEZONE)
  val arrowSchema = ArrowUtils2.toArrowSchema(schema, timezoneId)
  val allocator =
    ArrowUtils2.rootAllocator.newChildAllocator("row2ArrowBatchWrite", 0, Long.MaxValue)
  val root = VectorSchemaRoot.create(arrowSchema, allocator)
  val arrowBuffer = ArrowWriter.create(root)
  private val leLength = new Array[Byte](8)

  /** The file channel, used for repositioning / truncating the file. */
  private var channel: FileChannel = null
  private var mcs: ManualCloseOutputStream = null
  private var fos: FileOutputStream = null
  private var ts: TimeTrackingOutputStream = null
  private var initialized = false
  private var streamOpen = false
  private var hasBeenClosed = false

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
  private var partitionStartPos = 0L
  private var currentRowCount = 0
  private var writer: ArrowFileWriter = null

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
        mcs.manualClose()
      } {
        channel = null
        mcs = null
        fos = null
        ts = null
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

      if (syncWrites) {
        // Force outstanding writes to disk and track how long it takes
        val start = System.nanoTime()
        fos.getFD.sync()
        writeMetrics.incWriteTime(System.nanoTime() - start)
      }

      val pos = channel.position()
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

  def endPartition(): Unit = {
    if (currentRowCount > 0) {
      arrowBuffer.finish()
      writer.writeBatch()
      currentRowCount = 0
    }
    writer.end()
    val last = channel.position()
    MessageSerializer.longToBytes(last - partitionStartPos, leLength)
    channel.write(ByteBuffer.wrap(leLength))
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
            "Exception occurred while reverting partial writes to file "
              + file + ", " + ce.getMessage)
        case e: Exception =>
          logError("Uncaught exception while reverting partial writes to file " + file, e)
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

    currentRowCount += 1
    arrowBuffer.write(row)
    if (currentRowCount == maxRecordsPerBatch) {
      arrowBuffer.finish()
      writer.writeBatch()

      arrowBuffer.reset()
      currentRowCount = 0
    }
  }

  def open(): DiskBlockArrowIPCWriter = {
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
    fos = new FileOutputStream(file, true)
    channel = fos.getChannel()
    ts = new TimeTrackingOutputStream(writeMetrics, fos)
    class ManualCloseBufferedOutputStream
        extends BufferedOutputStream(ts, bufferSize)
        with ManualCloseOutputStream
    mcs = new ManualCloseBufferedOutputStream
  }

  def startPartition(): Unit = {
    partitionStartPos = channel.position()
    writer = new ArrowFileWriter(root, new MapDictionaryProvider(), channel)
    writer.start()
  }

  /**
   * Notify the writer that a record worth of bytes has been written with OutputStream#write.
   */
  def recordWritten(): Unit = {
    numRecordsWritten += 1
    writeMetrics.incRecordsWritten(1)

    if (numRecordsWritten % 16384 == 0) {
      updateBytesWritten()
    }
  }

  /**
   * Report the number of bytes written in this writer's shuffle write metrics.
   * Note that this is only valid before the underlying streams are closed.
   */
  private def updateBytesWritten(): Unit = {
    val pos = channel.position()
    writeMetrics.incBytesWritten(pos - reportedPosition)
    reportedPosition = pos
  }

  /**
   * Guards against close calls, e.g. from a wrapping stream.
   * Call manualClose to close the stream that was extended by this trait.
   * Commit uses this trait to close object streams without paying the
   * cost of closing and opening the underlying file.
   */
  private trait ManualCloseOutputStream extends OutputStream {
    abstract override def close(): Unit = {
      flush()
    }

    def manualClose(): Unit = {
      super.close()
    }
  }
}
