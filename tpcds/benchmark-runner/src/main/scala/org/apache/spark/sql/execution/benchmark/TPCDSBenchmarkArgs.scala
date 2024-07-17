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

class TPCDSBenchmarkArgs(val args: Array[String]) {
  var dataLocation: String = _
  var outputDir: String = _
  var queryFilter: Set[String] = Set.empty
  var round: Int = 2

  parseArgs(args.toList)
  validateArguments()

  private def optionMatch(optionName: String, s: String): Boolean = {
    optionName == s.toLowerCase(Locale.ROOT)
  }

  private def parseArgs(inputArgs: List[String]): Unit = {
    var args = inputArgs

    while (args.nonEmpty) {
      args match {
        case optName :: value :: tail if optionMatch("--data-location", optName) =>
          dataLocation = value
          args = tail

        case optName :: value :: tail if optionMatch("--output-dir", optName) =>
          outputDir = value
          args = tail

        case optName :: value :: tail if optionMatch("--query-filter", optName) =>
          queryFilter = value.toLowerCase(Locale.ROOT).split(",").map(_.trim).toSet
          args = tail

        case optName :: value :: tail if optionMatch("--round", optName) =>
          round = value.toInt
          args = tail

        case _ =>
          System.err.println("Unknown/unsupported param " + args)
          printUsageAndExit(1)
      }
    }
  }

  private def printUsageAndExit(exitCode: Int): Unit = {
    System.err.println("""
      |Usage: spark-submit --class <this class> <spark sql test jar> [Options]
      |Options:
      |  --data-location Path to TPCDS data
      |  --output-dir    Output directory for results
      |  --query-filter  Queries to filter, e.g., q3,q5,q13
      |  --round         Run each query for a specified number of rounds, default: 2
      |    """.stripMargin)
    System.exit(exitCode)
  }

  private def validateArguments(): Unit = {
    if (dataLocation == null) {
      System.err.println("Must specify a data location")
      printUsageAndExit(-1)
    }
    if (outputDir == null) {
      System.err.println("Must specify an output dir")
      printUsageAndExit(-1)
    }
  }
}
