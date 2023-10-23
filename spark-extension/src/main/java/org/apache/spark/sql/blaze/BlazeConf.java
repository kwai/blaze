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

public class BlazeConf {
    /// suggested batch size for arrow batches.
    public static int batchSize() {
        return intConf("spark.blaze.batchSize", 10000);
    }

    /// suggested fraction of off-heap memory used in native execution.
    /// actual off-heap memory usage is expected to be spark.executor.memoryOverhead * fraction.
    public static double memoryFraction() {
        return doubleConf("spark.blaze.memoryFraction", 0.6);
    }

    /// translates inequality smj to native. improves performance in most cases, however some
    /// issues are found in special cases, like tpcds q72.
    public static boolean enableSmjInequalityJoin() {
        return booleanConf("spark.blaze.enable.smjInequalityJoin", false);
    }

    /// fallbacks to SortMergeJoin when executing BroadcastHashJoin with big broadcasted table.
    public static boolean enableBhjFallbacksToSmj() {
        return booleanConf("spark.blaze.enable.bhjFallbacksToSmj", true);
    }

    /// fallbacks to SortMergeJoin when BroadcastHashJoin has a broadcasted table with rows more
    /// than this threshold. requires spark.blaze.enable.bhjFallbacksToSmj = true.
    public static int bhjFallbacksToSmjRowsThreshold() {
        return intConf("spark.blaze.bhjFallbacksToSmj.rows", 1000000);
    }

    /// fallbacks to SortMergeJoin when BroadcastHashJoin has a broadcasted table with memory usage
    /// more than this threshold. requires spark.blaze.enable.bhjFallbacksToSmj = true.
    public static int bhjFallbacksToSmjMemThreshold() {
        return intConf("spark.blaze.bhjFallbacksToSmj.mem.bytes", 134217728);
    }

    /// enable converting upper/lower functions to native, special cases may provide different
    /// outputs from spark due to different unicode versions.
    public static boolean enableCaseConvertFunctions() {
        return booleanConf("spark.blaze.enable.caseconvert.functions", false);
    }

    public static int udfWrapperNumThreads() {
        return intConf("spark.blaze.udfWrapperNumThreads", 1);
    }

    private static int intConf(String key, int defaultValue) {
        return conf().getInt(key, defaultValue);
    }

    private static double doubleConf(String key, double defaultValue) {
        return conf().getDouble(key, defaultValue);
    }

    private static boolean booleanConf(String key, boolean defaultValue) {
        return conf().getBoolean(key, defaultValue);
    }

    private static SparkConf conf() {
        return SparkEnv$.MODULE$.get().conf();
    }
}
