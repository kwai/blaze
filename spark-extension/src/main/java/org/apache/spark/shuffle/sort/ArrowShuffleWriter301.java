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

package org.apache.spark.shuffle.sort;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import javax.annotation.Nullable;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.annotation.Private;
import org.apache.spark.executor.KwaiExecutionPhaseMetrics;
import org.apache.spark.internal.config.package$;
import org.apache.spark.io.CompressionCodec;
import org.apache.spark.io.CompressionCodec$;
import org.apache.spark.io.NioBufferedFileInputStream;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.network.util.LimitedInputStream;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.IndexShuffleBlockResolver;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.execution.blaze.shuffle.ArrowShuffleDependency;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.BlockManagerId;
import org.apache.spark.storage.ShuffleDataBlockId;
import org.apache.spark.storage.TimeTrackingOutputStream;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.util.ExternalBlockStoreUtils;
import org.apache.spark.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Product2;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

@Private
public class ArrowShuffleWriter301<K, V> extends ShuffleWriter<K, V> {

  private static final Logger logger = LoggerFactory.getLogger(UnsafeShuffleWriter.class);

  private static final ClassTag<Object> OBJECT_CLASS_TAG = ClassTag$.MODULE$.Object();

  @VisibleForTesting static final int DEFAULT_INITIAL_SORT_BUFFER_SIZE = 4096;
  static final int DEFAULT_INITIAL_SER_BUFFER_SIZE = 1024 * 1024;

  private final BlockManager blockManager;
  private final IndexShuffleBlockResolver shuffleBlockResolver;
  private final TaskMemoryManager memoryManager;
  private final SerializerInstance serializer;
  private final Partitioner partitioner;
  private final ShuffleWriteMetricsReporter writeMetrics;
  private final int shuffleId;
  // Total task count in current shuffle.
  private final int numMaps;
  private final int mapId;
  private final TaskContext taskContext;
  private final SparkConf sparkConf;
  private final boolean transferToEnabled;
  private final int initialSortBufferSize;
  private final int inputBufferSizeInBytes;
  private final int outputBufferSizeInBytes;
  private final StructType schema;
  private final UnsafeProjection unsafeProjection;
  private final int maxRecordsPerBatch;

  @Nullable private MapStatus mapStatus;
  @Nullable private ArrowShuffleExternalSorter301 sorter;
  private long peakMemoryUsedBytes = 0;

  /** Subclass of ByteArrayOutputStream that exposes `buf` directly. */
  private static final class MyByteArrayOutputStream extends ByteArrayOutputStream {
    MyByteArrayOutputStream(int size) {
      super(size);
    }

    public byte[] getBuf() {
      return buf;
    }
  }

  private MyByteArrayOutputStream serBuffer;
  private SerializationStream serOutputStream;

  /**
   * Are we in the process of stopping? Because map tasks can call stop() with success = true and
   * then call stop() with success = false if they get an exception, we want to make sure we don't
   * try deleting files, etc twice.
   */
  private boolean stopping = false;

  private class CloseAndFlushShieldOutputStream extends CloseShieldOutputStream {

    CloseAndFlushShieldOutputStream(OutputStream outputStream) {
      super(outputStream);
    }

    @Override
    public void flush() {
      // do nothing
    }
  }

  public ArrowShuffleWriter301(
      BlockManager blockManager,
      IndexShuffleBlockResolver shuffleBlockResolver,
      TaskMemoryManager memoryManager,
      SerializedShuffleHandle<K, V> handle,
      int mapId,
      TaskContext taskContext,
      SparkConf sparkConf,
      ShuffleWriteMetricsReporter writeMetrics)
      throws IOException {
    final int numPartitions = handle.dependency().partitioner().numPartitions();
    if (numPartitions > SortShuffleManager.MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE()) {
      throw new IllegalArgumentException(
          "UnsafeShuffleWriter can only be used for shuffles with at most "
              + SortShuffleManager.MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE()
              + " reduce partitions");
    }
    this.blockManager = blockManager;
    this.shuffleBlockResolver = shuffleBlockResolver;
    this.memoryManager = memoryManager;
    this.mapId = mapId;
    this.numMaps = handle.numMaps();
    final ShuffleDependency<K, V, V> dep = handle.dependency();
    this.shuffleId = dep.shuffleId();
    this.serializer = dep.serializer().newInstance();
    this.partitioner = dep.partitioner();
    this.schema = ((ArrowShuffleDependency) dep).schema();
    this.unsafeProjection = UnsafeProjection.create(this.schema);
    this.writeMetrics = writeMetrics;
    this.taskContext = taskContext;
    this.sparkConf = sparkConf;
    this.transferToEnabled = sparkConf.getBoolean("spark.file.transferTo", true);
    this.initialSortBufferSize =
        sparkConf.getInt("spark.shuffle.sort.initialBufferSize", DEFAULT_INITIAL_SORT_BUFFER_SIZE);
    this.inputBufferSizeInBytes =
        (int) (long) sparkConf.get(package$.MODULE$.SHUFFLE_FILE_BUFFER_SIZE()) * 1024;
    this.outputBufferSizeInBytes =
        (int) (long) sparkConf.get(package$.MODULE$.SHUFFLE_UNSAFE_FILE_OUTPUT_BUFFER_SIZE())
            * 1024;
    this.maxRecordsPerBatch = sparkConf.getInt("spark.blaze.batchSize", 10000);
    open();
  }

  private void updatePeakMemoryUsed() {
    // sorter can be null if this writer is closed
    if (sorter != null) {
      long mem = sorter.getPeakMemoryUsedBytes();
      if (mem > peakMemoryUsedBytes) {
        peakMemoryUsedBytes = mem;
      }
    }
  }

  /** Return the peak memory used so far, in bytes. */
  public long getPeakMemoryUsedBytes() {
    updatePeakMemoryUsed();
    return peakMemoryUsedBytes;
  }

  /** This convenience method should only be called in test code. */
  @VisibleForTesting
  public void write(Iterator<Product2<K, V>> records) throws IOException {
    write(JavaConverters.asScalaIteratorConverter(records).asScala());
  }

  @Override
  public void write(scala.collection.Iterator<Product2<K, V>> records) throws IOException {
    // Keep track of success so we know if we encountered an exception
    // We do this rather than a standard try/catch/re-throw to handle
    // generic throwables.
    boolean success = false;
    try {
      while (records.hasNext()) {
        insertRecordIntoSorter(records.next());
      }
      closeAndWriteOutput();
      success = true;
    } finally {
      if (sorter != null) {
        try {
          sorter.cleanupResources();
        } catch (Exception e) {
          // Only throw this error if we won't be masking another
          // error.
          if (success) {
            throw e;
          } else {
            logger.error(
                "In addition to a failure during writing, we failed during " + "cleanup.", e);
          }
        }
      }
    }
  }

  private void open() {
    assert (sorter == null);
    sorter =
        new ArrowShuffleExternalSorter301(
            memoryManager,
            blockManager,
            taskContext,
            initialSortBufferSize,
            partitioner.numPartitions(),
            sparkConf,
            writeMetrics,
            schema,
            maxRecordsPerBatch);
    serBuffer = new MyByteArrayOutputStream(DEFAULT_INITIAL_SER_BUFFER_SIZE);
    serOutputStream = serializer.serializeStream(serBuffer);
  }

  @VisibleForTesting
  void closeAndWriteOutput() throws IOException {
    assert (sorter != null);
    updatePeakMemoryUsed();
    serBuffer = null;
    serOutputStream = null;
    final SpillInfo[] spills = sorter.closeAndGetSpills();
    sorter = null;
    final MapInfo mapInfo;
    final File output = shuffleBlockResolver.getDataFile(shuffleId, mapId);
    final File tmp = Utils.tempFileWith(output);
    BlockManagerId statusLoc = blockManager.shuffleServerId();
    boolean hasExternalData = false;
    long commitStart = System.currentTimeMillis();
    long mergeCost = 0;
    try {
      try {
        long mergeStart = System.currentTimeMillis();
        mapInfo = mergeSpills(spills, tmp);
        long mergeEnd = System.currentTimeMillis();
        mergeCost = mergeEnd - mergeStart;
      } finally {
        for (SpillInfo spill : spills) {
          if (spill.file.exists() && !spill.file.delete()) {
            logger.error("Error while deleting spill file {}", spill.file.getPath());
          }
        }
      }
      shuffleBlockResolver.writeIndexFileAndCommit(shuffleId, mapId, mapInfo.lengths, tmp);
      long totalPartitionLength = 0L;
      for (int i = 0; i < mapInfo.lengths.length; i++) {
        totalPartitionLength += mapInfo.lengths[i];
      }
      if (ExternalBlockStoreUtils.writeRemoteEnabled(
          SparkEnv.get().conf(), totalPartitionLength, numMaps)) {
        if (output.length() != 0) {
          logger.info(
              "KwaiShuffle: Start to write remote, shuffle block id "
                  + new ShuffleDataBlockId(shuffleId, mapId, 0).name()
                  + ", file path is "
                  + output.getPath()
                  + "/"
                  + output.getName());
          taskContext.taskMetrics().externalMetrics().writeRemoteShuffle().setValue(1L);
          BlockId dataBlockId =
              new ShuffleDataBlockId(shuffleId, mapId, IndexShuffleBlockResolver.NOOP_REDUCE_ID());
          File indexFile = shuffleBlockResolver.getIndexFile(shuffleId, mapId);
          blockManager
              .externalBlockStore()
              .externalBlockManager()
              .get()
              .writeExternalShuffleFile(shuffleId, mapId, dataBlockId, indexFile, output);
          hasExternalData = true;
        }
      }
    } finally {
      if (tmp.exists() && !tmp.delete()) {
        logger.error("Error while deleting temp file {}", tmp.getAbsolutePath());
      }
    }
    long commitEnd = System.currentTimeMillis();
    taskContext
        .taskMetrics()
        .executionPhaseMetric()
        .incShuffleWriteTime(commitEnd - commitStart - mergeCost);
    mapStatus = MapStatus$.MODULE$.apply(statusLoc, mapInfo.lengths, mapInfo.records);
    mapStatus.hasExternal_$eq(hasExternalData);
  }

  @VisibleForTesting
  void insertRecordIntoSorter(Product2<K, V> record) throws IOException {
    assert (sorter != null);
    final K key = record._1();
    final int partitionId = partitioner.getPartition(key);
    serBuffer.reset();
    serOutputStream.writeKey(key, OBJECT_CLASS_TAG);
    if (record._2() instanceof UnsafeRow) {
      serOutputStream.writeValue(record._2(), OBJECT_CLASS_TAG);
    } else {
      UnsafeRow unsafeRow = unsafeProjection.apply((InternalRow) record._2());
      serOutputStream.writeValue(unsafeRow, OBJECT_CLASS_TAG);
    }
    serOutputStream.flush();

    final int serializedRecordSize = serBuffer.size();
    assert (serializedRecordSize > 0);

    sorter.insertRecord(
        serBuffer.getBuf(), Platform.BYTE_ARRAY_OFFSET, serializedRecordSize, partitionId);
  }

  @VisibleForTesting
  void forceSorterToSpill() throws IOException {
    assert (sorter != null);
    sorter.spill();
  }

  /**
   * Merge zero or more spill files together, choosing the fastest merging strategy based on the
   * number of spills and the IO compression codec.
   *
   * @return the partition lengths in the merged file.
   */
  private MapInfo mergeSpills(SpillInfo[] spills, File outputFile) throws IOException {
    final boolean compressionEnabled = sparkConf.getBoolean("spark.shuffle.compress", true);
    final CompressionCodec compressionCodec = CompressionCodec$.MODULE$.createCodec(sparkConf);
    final boolean fastMergeEnabled =
        sparkConf.getBoolean("spark.shuffle.unsafe.fastMergeEnabled", true);
    final boolean fastMergeIsSupported =
        !compressionEnabled
            || CompressionCodec$.MODULE$.supportsConcatenationOfSerializedStreams(compressionCodec);
    final boolean encryptionEnabled = blockManager.serializerManager().encryptionEnabled();
    try {
      if (spills.length == 0) {
        new FileOutputStream(outputFile).close(); // Create an empty file
        return new MapInfo(
            new long[partitioner.numPartitions()], new long[partitioner.numPartitions()]);
      } else if (spills.length == 1) {
        // Here, we don't need to perform any metrics updates because the bytes written to this
        // output file would have already been counted as shuffle bytes written.
        Files.move(spills[0].file, outputFile);
        return new MapInfo(spills[0].partitionLengths, spills[0].partitionRecords);
      } else {
        final MapInfo mapInfo;
        // There are multiple spills to merge, so none of these spill files' lengths were counted
        // towards our shuffle write count or shuffle write time. If we use the slow merge path,
        // then the final output file's size won't necessarily be equal to the sum of the spill
        // files' sizes. To guard against this case, we look at the output file's actual size when
        // computing shuffle bytes written.
        //
        // We allow the individual merge methods to report their own IO times since different merge
        // strategies use different IO techniques.  We count IO during merge towards the shuffle
        // shuffle write time, which appears to be consistent with the "not bypassing merge-sort"
        // branch in ExternalSorter.
        if (fastMergeEnabled && fastMergeIsSupported) {
          // Compression is disabled or we are using an IO compression codec that supports
          // decompression of concatenated compressed streams, so we can perform a fast spill merge
          // that doesn't need to interpret the spilled bytes.
          if (transferToEnabled && !encryptionEnabled) {
            logger.debug("Using transferTo-based fast merge");
            logger.info("merge shuffle spill file, using transferTo-based fast merge");
            mapInfo = mergeSpillsWithTransferTo(spills, outputFile);
          } else {
            logger.debug("Using fileStream-based fast merge");
            logger.info("merge shuffle spill file, using fileStream-based fast merge");
            mapInfo = mergeSpillsWithFileStream(spills, outputFile, null);
          }
        } else {
          logger.debug("Using slow merge");
          logger.info("merge shuffle spill file, using slow merge");
          mapInfo = mergeSpillsWithFileStream(spills, outputFile, compressionCodec);
        }
        // When closing an UnsafeShuffleExternalSorter that has already spilled once but also has
        // in-memory records, we write out the in-memory records to a file but do not count that
        // final write as bytes spilled (instead, it's accounted as shuffle write). The merge needs
        // to be counted as shuffle write, but this will lead to double-counting of the final
        // SpillInfo's bytes.
        writeMetrics.decBytesWritten(spills[spills.length - 1].file.length());
        writeMetrics.incBytesWritten(outputFile.length());
        return mapInfo;
      }
    } catch (IOException e) {
      if (outputFile.exists() && !outputFile.delete()) {
        logger.error("Unable to delete output file {}", outputFile.getPath());
      }
      throw e;
    }
  }

  /**
   * Merges spill files using Java FileStreams. This code path is typically slower than the
   * NIO-based merge, {@link UnsafeShuffleWriter#mergeSpillsWithTransferTo(SpillInfo[], File)}, and
   * it's mostly used in cases where the IO compression codec does not support concatenation of
   * compressed data, when encryption is enabled, or when users have explicitly disabled use of
   * {@code transferTo} in order to work around kernel bugs. This code path might also be faster in
   * cases where individual partition size in a spill is small and
   * UnsafeShuffleWriter#mergeSpillsWithTransferTo method performs many small disk ios which is
   * inefficient. In those case, Using large buffers for input and output files helps reducing the
   * number of disk ios, making the file merging faster.
   *
   * @param spills the spills to merge.
   * @param outputFile the file to write the merged data to.
   * @param compressionCodec the IO compression codec, or null if shuffle compression is disabled.
   * @return the partition lengths in the merged file.
   */
  private MapInfo mergeSpillsWithFileStream(
      SpillInfo[] spills, File outputFile, @Nullable CompressionCodec compressionCodec)
      throws IOException {
    assert (spills.length >= 2);
    final int numPartitions = partitioner.numPartitions();
    final long[] partitionLengths = new long[numPartitions];
    final long[] partitionRecords = new long[numPartitions];
    final InputStream[] spillInputStreams = new InputStream[spills.length];

    final OutputStream bos =
        new BufferedOutputStream(new FileOutputStream(outputFile), outputBufferSizeInBytes);
    // Use a counting output stream to avoid having to close the underlying file and ask
    // the file system for its size after each partition is written.
    final CountingOutputStream mergedFileOutputStream = new CountingOutputStream(bos);

    boolean threwException = true;
    try {
      KwaiExecutionPhaseMetrics shuffleSpillMetric =
          taskContext.taskMetrics().getExecutionPhaseMetrics();
      for (int i = 0; i < spills.length; i++) {
        spillInputStreams[i] =
            new NioBufferedFileInputStream(
                spills[i].file, inputBufferSizeInBytes, shuffleSpillMetric, true);
      }
      for (int partition = 0; partition < numPartitions; partition++) {
        final long initialFileLength = mergedFileOutputStream.getByteCount();
        // Shield the underlying output stream from close() and flush() calls, so that we can close
        // the higher level streams to make sure all data is really flushed and internal state is
        // cleaned.
        OutputStream partitionOutput =
            new CloseAndFlushShieldOutputStream(
                new TimeTrackingOutputStream(writeMetrics, mergedFileOutputStream));
        partitionOutput = blockManager.serializerManager().wrapForEncryption(partitionOutput);
        if (compressionCodec != null) {
          partitionOutput = compressionCodec.compressedOutputStream(partitionOutput);
        }
        long records = 0;
        for (int i = 0; i < spills.length; i++) {
          final long partitionLengthInSpill = spills[i].partitionLengths[partition];
          if (partitionLengthInSpill > 0) {
            InputStream partitionInputStream =
                new LimitedInputStream(spillInputStreams[i], partitionLengthInSpill, false);
            try {
              partitionInputStream =
                  blockManager.serializerManager().wrapForEncryption(partitionInputStream);
              if (compressionCodec != null) {
                partitionInputStream = compressionCodec.compressedInputStream(partitionInputStream);
              }
              ByteStreams.copy(partitionInputStream, partitionOutput);
            } finally {
              partitionInputStream.close();
            }
          }
          records += spills[i].partitionRecords[partition];
        }
        partitionOutput.flush();
        partitionOutput.close();
        partitionLengths[partition] = (mergedFileOutputStream.getByteCount() - initialFileLength);
        partitionRecords[partition] = records;
      }
      threwException = false;
    } finally {
      // To avoid masking exceptions that caused us to prematurely enter the finally block, only
      // throw exceptions during cleanup if threwException == false.
      for (InputStream stream : spillInputStreams) {
        Closeables.close(stream, threwException);
      }
      Closeables.close(mergedFileOutputStream, threwException);
    }
    taskContext
        .taskMetrics()
        .executionPhaseMetric()
        .incShuffleWriteTime(writeMetrics.writeTime() / 1000000);
    return new MapInfo(partitionLengths, partitionRecords);
  }

  /**
   * Merges spill files by using NIO's transferTo to concatenate spill partitions' bytes. This is
   * only safe when the IO compression codec and serializer support concatenation of serialized
   * streams.
   *
   * @return the partition lengths in the merged file.
   */
  private MapInfo mergeSpillsWithTransferTo(SpillInfo[] spills, File outputFile)
      throws IOException {
    assert (spills.length >= 2);
    final long writeStartTime = System.nanoTime();
    final int numPartitions = partitioner.numPartitions();
    final long[] partitionLengths = new long[numPartitions];
    final long[] partitionRecords = new long[numPartitions];
    final FileChannel[] spillInputChannels = new FileChannel[spills.length];
    final long[] spillInputChannelPositions = new long[spills.length];
    FileChannel mergedFileOutputChannel = null;

    boolean threwException = true;
    try {
      for (int i = 0; i < spills.length; i++) {
        spillInputChannels[i] = new FileInputStream(spills[i].file).getChannel();
      }
      // This file needs to opened in append mode in order to work around a Linux kernel bug that
      // affects transferTo; see SPARK-3948 for more details.
      mergedFileOutputChannel = new FileOutputStream(outputFile, true).getChannel();

      long bytesWrittenToMergedFile = 0;
      for (int partition = 0; partition < numPartitions; partition++) {
        for (int i = 0; i < spills.length; i++) {
          final long partitionLengthInSpill = spills[i].partitionLengths[partition];
          final long partitionRecordInSpill = spills[i].partitionRecords[partition];
          final FileChannel spillInputChannel = spillInputChannels[i];
          Utils.copyFileStreamNIO(
              spillInputChannel,
              mergedFileOutputChannel,
              spillInputChannelPositions[i],
              partitionLengthInSpill);
          spillInputChannelPositions[i] += partitionLengthInSpill;
          bytesWrittenToMergedFile += partitionLengthInSpill;
          partitionLengths[partition] += partitionLengthInSpill;
          partitionRecords[partition] += partitionRecordInSpill;
        }
      }
      // Check the position after transferTo loop to see if it is in the right position and raise an
      // exception if it is incorrect. The position will not be increased to the expected length
      // after calling transferTo in kernel version 2.6.32. This issue is described at
      // https://bugs.openjdk.java.net/browse/JDK-7052359 and SPARK-3948.
      if (mergedFileOutputChannel.position() != bytesWrittenToMergedFile) {
        throw new IOException(
            "Current position "
                + mergedFileOutputChannel.position()
                + " does not equal expected "
                + "position "
                + bytesWrittenToMergedFile
                + " after transferTo. Please check your kernel"
                + " version to see if it is 2.6.32, as there is a kernel bug which will lead to "
                + "unexpected behavior when using transferTo. You can set spark.file.transferTo=false "
                + "to disable this NIO feature.");
      }
      threwException = false;
    } finally {
      // To avoid masking exceptions that caused us to prematurely enter the finally block, only
      // throw exceptions during cleanup if threwException == false.
      for (int i = 0; i < spills.length; i++) {
        assert (spillInputChannelPositions[i] == spills[i].file.length());
        Closeables.close(spillInputChannels[i], threwException);
      }
      Closeables.close(mergedFileOutputChannel, threwException);
    }
    writeMetrics.incWriteTime(System.nanoTime() - writeStartTime);
    taskContext
        .taskMetrics()
        .executionPhaseMetric()
        .incShuffleWriteTime((System.nanoTime() - writeStartTime) / 1000000);
    return new MapInfo(partitionLengths, partitionRecords);
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    try {
      taskContext.taskMetrics().incPeakExecutionMemory(getPeakMemoryUsedBytes());

      if (stopping) {
        return Option.apply(null);
      } else {
        stopping = true;
        if (success) {
          if (mapStatus == null) {
            throw new IllegalStateException("Cannot call stop(true) without having called write()");
          }
          return Option.apply(mapStatus);
        } else {
          return Option.apply(null);
        }
      }
    } finally {
      if (sorter != null) {
        // If sorter is non-null, then this implies that we called stop() in response to an error,
        // so we need to clean up memory and spill files created by the sorter
        sorter.cleanupResources();
      }
    }
  }
}
