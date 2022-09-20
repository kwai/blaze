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

package org.apache.spark.sql.execution.blaze.shuffle;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Closeables;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.spark.*;
import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.executor.KwaiExternalMetrics;
import org.apache.spark.internal.config.package$;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.IndexShuffleBlockResolver;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.shuffle.sort.BypassMergeSortShuffleHandle;
import org.apache.spark.shuffle.sort.MapInfo;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.*;
import org.apache.spark.util.ExternalBlockStoreUtils;
import org.apache.spark.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.*;
import scala.collection.Iterator;

/**
 * This class implements sort-based shuffle's hash-style shuffle fallback path. This write path
 * writes incoming records to separate files, one file per reduce partition, then concatenates these
 * per-partition files to form a single output file, regions of which are served to reducers.
 * Records are not buffered in memory. It writes output in a format that can be served / consumed
 * via {@link org.apache.spark.shuffle.IndexShuffleBlockResolver}.
 *
 * <p>This write path is inefficient for shuffles with large numbers of reduce partitions because it
 * simultaneously opens separate serializers and file streams for all partitions. As a result,
 * {@link SortShuffleManager} only selects this write path when
 *
 * <ul>
 *   <li>no Ordering is specified,
 *   <li>no Aggregator is specified, and
 *   <li>the number of partitions is less than <code>spark.shuffle.sort.bypassMergeThreshold</code>.
 * </ul>
 *
 * This code used to be part of {@link org.apache.spark.util.collection.ExternalSorter} but was
 * refactored into its own class in order to reduce code complexity; see SPARK-7855 for details.
 *
 * <p>There have been proposals to completely remove this code path; see SPARK-6026 for details.
 */
final class ArrowBypassMergeSortShuffleWriter<K, V> extends ShuffleWriter<K, V> {

  private static final Logger logger =
      LoggerFactory.getLogger(ArrowBypassMergeSortShuffleWriter.class);

  private final int fileBufferSize;
  private final boolean transferToEnabled;
  private final int numPartitions;
  private final BlockManager blockManager;
  private final Partitioner partitioner;
  private final ShuffleWriteMetricsReporter writeMetrics;
  private final KwaiExternalMetrics kwaiExternalMetrics;
  private final int shuffleId;
  private final int numMaps;
  private final int mapId;
  private final Serializer serializer;
  private final IndexShuffleBlockResolver shuffleBlockResolver;
  private final StructType schema;
  private final int maxRecordsPerBatch;
  private final boolean shuffleSync;

  /** Array of file writers, one for each partition */
  private DiskBlockArrowIPCWriter[] partitionWriters;

  private FileSegment[] partitionWriterSegments;
  @Nullable private MapStatus mapStatus;
  private long[] partitionLengths;

  private boolean checkSpillFileLength;

  /**
   * Are we in the process of stopping? Because map tasks can call stop() with success = true and
   * then call stop() with success = false if they get an exception, we want to make sure we don't
   * try deleting files, etc twice.
   */
  private boolean stopping = false;

  ArrowBypassMergeSortShuffleWriter(
      BlockManager blockManager,
      IndexShuffleBlockResolver shuffleBlockResolver,
      BypassMergeSortShuffleHandle<K, V> handle,
      int mapId,
      TaskContext taskContext,
      SparkConf conf,
      ShuffleWriteMetricsReporter writeMetrics) {
    // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
    this.fileBufferSize = (int) conf.getSizeAsKb("spark.shuffle.file.buffer", "32k") * 1024;
    this.transferToEnabled = conf.getBoolean("spark.file.transferTo", true);
    this.blockManager = blockManager;
    final ShuffleDependency<K, V, V> dep = handle.dependency();
    this.mapId = mapId;
    this.numMaps = handle.numMaps();
    this.shuffleId = dep.shuffleId();
    this.partitioner = dep.partitioner();
    this.numPartitions = partitioner.numPartitions();
    this.writeMetrics = writeMetrics;
    this.kwaiExternalMetrics = taskContext.taskMetrics().externalMetrics();
    this.serializer = dep.serializer();
    this.shuffleBlockResolver = shuffleBlockResolver;
    this.checkSpillFileLength =
        (boolean) conf.get(package$.MODULE$.SHUFFLE_CHECK_SPILL_FILE_LENGTH_ENABLED());
    this.schema = ((ArrowShuffleDependency) dep).schema();
    this.maxRecordsPerBatch = conf.getInt("spark.blaze.shuffle.maxRecordsPerBatch", 8192);
    this.shuffleSync = conf.getBoolean("spark.shuffle.sync", false);
  }

  @Override
  public void write(Iterator<Product2<K, V>> records) throws IOException {
    assert (partitionWriters == null);
    try {
      if (!records.hasNext()) {
        partitionLengths = new long[numPartitions];
        shuffleBlockResolver.writeIndexFileAndCommit(shuffleId, mapId, partitionLengths, null);
        mapStatus =
            MapStatus$.MODULE$.apply(
                blockManager.shuffleServerId(), partitionLengths, new long[numPartitions]);
        return;
      }
      final SerializerInstance serInstance = serializer.newInstance();
      final long openStartTime = System.nanoTime();
      partitionWriters = new DiskBlockArrowIPCWriter[numPartitions];
      partitionWriterSegments = new FileSegment[numPartitions];
      for (int i = 0; i < numPartitions; i++) {
        final Tuple2<TempShuffleBlockId, File> tempShuffleBlockIdPlusFile =
            blockManager.diskBlockManager().createTempShuffleBlock();
        final File file = tempShuffleBlockIdPlusFile._2();
        final BlockId blockId = tempShuffleBlockIdPlusFile._1();
        partitionWriters[i] =
            new DiskBlockArrowIPCWriter(
                file, fileBufferSize, shuffleSync, writeMetrics, schema, maxRecordsPerBatch);
      }
      // Creating the file to write to and creating a disk writer both involve interacting with
      // the disk, and can take a long time in aggregate when we open many files, so should be
      // included in the shuffle write time.
      writeMetrics.incWriteTime(System.nanoTime() - openStartTime);

      Set<Integer> nonEmptyPartition = new HashSet<>();
      while (records.hasNext()) {
        final Product2<K, V> record = records.next();
        final K key = record._1();
        int partitionId = partitioner.getPartition(key);
        partitionWriters[partitionId].write((InternalRow) record._2());
        nonEmptyPartition.add(partitionId);
      }

      for (int i = 0; i < numPartitions; i++) {
        final DiskBlockArrowIPCWriter writer = partitionWriters[i];
        FileSegment fileSegment = writer.commitAndGet();
        partitionWriterSegments[i] = fileSegment;
        File file = fileSegment.file();
        long fileRealLength = file.length();
        long writeLength = fileSegment.length();
        if (checkSpillFileLength && fileRealLength != writeLength) {
          throw new IOException(
              "Disk maybe broken leading shuffle spillInfo write length "
                  + writeLength
                  + " not match with spill file length "
                  + fileRealLength);
        }
        writer.close();
      }

      File output = shuffleBlockResolver.getDataFile(shuffleId, mapId);
      File tmp = Utils.tempFileWith(output);
      MapInfo mapInfo;
      boolean hasExternalData = false;
      BlockManagerId statusLoc = blockManager.shuffleServerId();
      try {
        mapInfo = writePartitionedFile(tmp, nonEmptyPartition);
        partitionLengths = mapInfo.lengths;
        shuffleBlockResolver.writeIndexFileAndCommit(shuffleId, mapId, partitionLengths, tmp);
        long totalPartitionLength = 0L;
        for (int i = 0; i < mapInfo.lengths.length; i++) {
          totalPartitionLength += mapInfo.lengths[i];
        }
        if (ExternalBlockStoreUtils.writeRemoteEnabled(
            SparkEnv.get().conf(), totalPartitionLength, numMaps)) {
          logger.info(
              "KwaiShuffle: Start to write remote, shuffle block id "
                  + new ShuffleDataBlockId(shuffleId, mapId, 0).name());
          kwaiExternalMetrics.writeRemoteShuffle().setValue(1L);
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
      } finally {
        if (tmp.exists() && !tmp.delete()) {
          logger.error("Error while deleting temp file {}", tmp.getAbsolutePath());
        }
      }
      mapStatus = MapStatus$.MODULE$.apply(statusLoc, mapInfo.lengths, mapInfo.records);
      mapStatus.hasExternal_$eq(hasExternalData);

    } catch (Exception e) {
      logger.error("Error while writing partitions", e);
      throw e;
    }
  }

  @VisibleForTesting
  long[] getPartitionLengths() {
    return partitionLengths;
  }

  /**
   * Concatenate all of the per-partition files into a single combined file.
   *
   * @return array of lengths, in bytes, of each partition of the file (used by map output tracker).
   */
  private MapInfo writePartitionedFile(File outputFile, Set<Integer> nonEmptyPartition)
      throws IOException {
    // Track location of the partition starts in the output file
    final long[] lengths = new long[numPartitions];
    final long[] records = new long[numPartitions];
    if (partitionWriters == null) {
      // We were passed an empty iterator
      return new MapInfo(lengths, records);
    }

    final FileOutputStream out = new FileOutputStream(outputFile, true);
    final long writeStartTime = System.nanoTime();
    boolean threwException = true;
    long mergeTotalBytes = 0L;
    try {
      for (int i = 0; i < numPartitions; i++) {
        if (nonEmptyPartition.contains(i)) {
          FileSegment fileSegments = partitionWriterSegments[i];
          final File file = fileSegments.file();
          long segmentFileLength = file.length();
          long segmentLength = fileSegments.length();
          records[i] = partitionWriterSegments[i].record();
          if (file.exists()) {
            final FileInputStream in = new FileInputStream(file);
            boolean copyThrewException = true;
            try {
              long copyLength = Utils.copyStream(in, out, false, transferToEnabled);
              mergeTotalBytes += copyLength;
              if (checkSpillFileLength && segmentLength != segmentFileLength
                  || segmentLength != copyLength) {
                throw new IOException(
                    "Disk maybe broken leading shuffle spillInfo write length "
                        + segmentLength
                        + " not match with spill file length "
                        + segmentFileLength
                        + " or copyStream length "
                        + copyLength);
              }
              lengths[i] = copyLength;
              copyThrewException = false;
            } finally {
              Closeables.close(in, copyThrewException);
            }
            if (!file.delete()) {
              logger.error("Unable to delete file for partition {}", i);
            }
          } else {
            throw new IOException(
                "Partition " + i + " segment file " + file.getAbsolutePath() + " not exists.");
          }
        }
      }
      threwException = false;
    } finally {
      Closeables.close(out, threwException);
      writeMetrics.incWriteTime(System.nanoTime() - writeStartTime);
    }
    if (mergeTotalBytes != outputFile.length()) {
      throw new IOException(
          "Disk maybe broken leading shuffle spill merge length "
              + mergeTotalBytes
              + " not match with merge file real length "
              + outputFile.length());
    }
    partitionWriters = null;
    return new MapInfo(lengths, records);
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    if (stopping) {
      return None$.empty();
    } else {
      stopping = true;
      if (success) {
        if (mapStatus == null) {
          throw new IllegalStateException("Cannot call stop(true) without having called write()");
        }
        return Option.apply(mapStatus);
      } else {
        // The map task failed, so delete our output data.
        if (partitionWriters != null) {
          try {
            for (DiskBlockArrowIPCWriter writer : partitionWriters) {
              // This method explicitly does _not_ throw exceptions:
              File file = writer.revertPartialWritesAndClose();
              if (!file.delete()) {
                logger.error("Error while deleting file {}", file.getAbsolutePath());
              }
            }
          } finally {
            partitionWriters = null;
          }
        }
        return None$.empty();
      }
    }
  }
}
