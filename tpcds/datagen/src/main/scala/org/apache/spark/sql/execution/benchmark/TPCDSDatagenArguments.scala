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

package org.apache.spark.sql.execution.benchmark

import java.util.Locale

import scala.util.Try

class TPCDSDatagenArguments(val args: Array[String]) {
  var outputLocation: String = null
  var scaleFactor = "1"
  var format = "parquet"
  var overwrite = false
  var partitionTables = false
  var useDoubleForDecimal = false
  var useStringForChar = false
  var clusterByPartitionColumns = false
  var filterOutNullPartitionValues = false
  var tableFilter: Set[String] = Set.empty
  var numPartitions = "100"

  parseArgs(args.toList)
  validateArguments()

  private def parseArgs(inputArgs: List[String]): Unit = {
    var args = inputArgs

    while(args.nonEmpty) {
      args match {
        case ("--output-location") :: value :: tail =>
          outputLocation = value
          args = tail

        case ("--scale-factor") :: value :: tail =>
          scaleFactor = value
          args = tail

        case ("--format") :: value :: tail =>
          format = value
          args = tail

        case ("--overwrite") :: tail =>
          overwrite = true
          args = tail

        case ("--partition-tables") :: tail =>
          partitionTables = true
          args = tail

        case ("--use-double-for-decimal") :: tail =>
          useDoubleForDecimal = true
          args = tail

        case ("--use-string-for-char") :: tail =>
          useStringForChar = true
          args = tail

        case ("--cluster-by-partition-columns") :: tail =>
          clusterByPartitionColumns = true
          args = tail

        case ("--filter-out-null-partition-values") :: tail =>
          filterOutNullPartitionValues = true
          args = tail

        case ("--table-filter") :: value :: tail =>
          tableFilter = value.toLowerCase(Locale.ROOT).split(",").map(_.trim).toSet
          args = tail

        case ("--num-partitions") :: value :: tail =>
          numPartitions = value
          args = tail

        case ("--help") :: tail =>
          printUsageAndExit(0)

        case _ =>
          // scalastyle:off println
          System.err.println("Unknown/unsupported param " + args)
          // scalastyle:on println
          printUsageAndExit(1)
      }
    }
  }

  private def printUsageAndExit(exitCode: Int): Unit = {
    // scalastyle:off
    System.err.println("""
      |Usage: spark-submit --class <this class> --conf key=value <spark tpcds datagen jar> [Options]
      |Options:
      |  --output-location [STR]                Path to an output location
      |  --scale-factor [NUM]                   Scale factor (default: 1)
      |  --format [STR]                         Output format (default: parquet)
      |  --overwrite                            Whether it overwrites existing data (default: false)
      |  --partition-tables                     Whether it partitions output data (default: false)
      |  --use-double-for-decimal               Whether it prefers double types instead of decimal types (default: false)
      |  --use-string-for-char                  Whether it prefers string types instead of char/varchar types (default: false)
      |  --cluster-by-partition-columns         Whether it cluster output data by partition columns (default: false)
      |  --filter-out-null-partition-values     Whether it filters out NULL partitions (default: false)
      |  --table-filter [STR]                   Queries to filter, e.g., catalog_sales,store_sales
      |  --num-partitions [NUM]                 # of partitions (default: 100)
      |
      """.stripMargin)
    // scalastyle:on
    System.exit(exitCode)
  }

  private def validateArguments(): Unit = {
    if (outputLocation == null) {
      // scalastyle:off println
      System.err.println("Must specify an output location")
      // scalastyle:on println
      printUsageAndExit(-1)
    }
    if (Try(scaleFactor.toInt).getOrElse(-1) <= 0) {
      // scalastyle:off println
      System.err.println("Scale factor must be a positive number")
      // scalastyle:on println
      printUsageAndExit(-1)
    }
    if (Try(numPartitions.toInt).getOrElse(-1) <= 0) {
      // scalastyle:off println
      System.err.println("Number of partitions must be a positive number")
      // scalastyle:on println
      printUsageAndExit(-1)
    }
  }
}
