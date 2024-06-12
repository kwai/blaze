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
package org.apache.spark.sql.blaze;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv$;

@SuppressWarnings("unused")
public enum BlazeConf {
    /// suggested batch size for arrow batches.
    BATCH_SIZE("spark.blaze.batchSize", 10000),

    /// suggested fraction of off-heap memory used in native execution.
    /// actual off-heap memory usage is expected to be spark.executor.memoryOverhead * fraction.
    MEMORY_FRACTION("spark.blaze.memoryFraction", 0.6),

    /// fallbacks to SortMergeJoin when executing BroadcastHashJoin with big broadcasted table.
    /// not available in blaze 3.0+
    BHJ_FALLBACKS_TO_SMJ_ENABLE("spark.blaze.enable.bhjFallbacksToSmj", false),

    /// fallbacks to SortMergeJoin when BroadcastHashJoin has a broadcasted table with rows more
    /// than this threshold. requires spark.blaze.enable.bhjFallbacksToSmj = true.
    /// not available in blaze 3.0+
    BHJ_FALLBACKS_TO_SMJ_ROWS_THRESHOLD("spark.blaze.bhjFallbacksToSmj.rows", 1000000),

    /// fallbacks to SortMergeJoin when BroadcastHashJoin has a broadcasted table with memory usage
    /// more than this threshold. requires spark.blaze.enable.bhjFallbacksToSmj = true.
    BHJ_FALLBACKS_TO_SMJ_MEM_THRESHOLD("spark.blaze.bhjFallbacksToSmj.mem.bytes", 134217728),

    /// enable converting upper/lower functions to native, special cases may provide different
    /// outputs from spark due to different unicode versions.
    CASE_CONVERT_FUNCTIONS_ENABLE("spark.blaze.enable.caseconvert.functions", true),

    /// number of threads evaluating UDFs
    /// improves performance for special case that UDF concurrency matters
    UDF_WRAPPER_NUM_THREADS("spark.blaze.udfWrapperNumThreads", 1),

    /// enable extra metrics of input batch statistics
    INPUT_BATCH_STATISTICS_ENABLE("spark.blaze.enableInputBatchStatistics", true),

    /// ignore corrupted input files
    IGNORE_CORRUPTED_FILES("spark.files.ignoreCorruptFiles", false),

    /// enable partial aggregate skipping (see https://github.com/blaze-init/blaze/issues/327)
    PARTIAL_AGG_SKIPPING_ENABLE("spark.blaze.partialAggSkipping.enable", true),

    /// partial aggregate skipping ratio
    PARTIAL_AGG_SKIPPING_RATIO("spark.blaze.partialAggSkipping.ratio", 0.8),

    /// mininum number of rows to trigger partial aggregate skipping
    PARTIAL_AGG_SKIPPING_MIN_ROWS("spark.blaze.partialAggSkipping.minRows", BATCH_SIZE.intConf() * 2),

    // parquet enable page filtering
    PARQUET_ENABLE_PAGE_FILTERING("spark.blaze.parquet.enable.pageFiltering", false),

    // parqeut enable bloom filter
    PARQUET_ENABLE_BLOOM_FILTER("spark.blaze.parquet.enable.bloomFilter", false),
    ;

    private String key;
    private Object defaultValue;

    BlazeConf(String key, Object defaultValue) {
        this.key = key;
        this.defaultValue = defaultValue;
    }

    public boolean booleanConf() {
        return conf().getBoolean(key, (boolean) defaultValue);
    }

    public int intConf() {
        return conf().getInt(key, (int) defaultValue);
    }

    public long longConf() {
        return conf().getLong(key, (long) defaultValue);
    }

    public double doubleConf() {
        return conf().getDouble(key, (double) defaultValue);
    }

    public static boolean booleanConf(String confName) {
        return BlazeConf.valueOf(confName).booleanConf();
    }

    public static int intConf(String confName) {
        return BlazeConf.valueOf(confName).intConf();
    }

    public static long longConf(String confName) {
        return BlazeConf.valueOf(confName).longConf();
    }

    public static double doubleConf(String confName) {
        return BlazeConf.valueOf(confName).doubleConf();
    }

    private static SparkConf conf() {
        return SparkEnv$.MODULE$.get().conf();
    }
}
