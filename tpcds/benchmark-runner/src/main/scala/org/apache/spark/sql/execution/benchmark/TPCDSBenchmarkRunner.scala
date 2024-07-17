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

import java.io.PrintWriter
import java.nio.file.Files
import java.nio.file.Paths
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row

object TPCDSBenchmarkRunner {
  def main(args: Array[String]): Unit = {
    val dt = new SimpleDateFormat("yyyyMMddHHmm").format(new Date())
    val benchmarkArgs = new TPCDSBenchmarkArgs(args)
    val dataLocation = Paths.get(benchmarkArgs.dataLocation)
    val outputDir = Paths.get(benchmarkArgs.outputDir, dt)
    val tpcdsQueries = Seq(
      "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10", "q11",
      "q12", "q13", "q14a", "q14b", "q15", "q16", "q17", "q18", "q19", "q20",
      "q21", "q22", "q23a", "q23b", "q24a", "q24b", "q25", "q26", "q27", "q28", "q29",
      "q30", "q31", "q32", "q33", "q34", "q35", "q36", "q37", "q38", "q39a", "q39b",
      "q40", "q41", "q42", "q43", "q44", "q45", "q46", "q47", "q48", "q49", "q50",
      "q51", "q52", "q53", "q54", "q55", "q56", "q57", "q58", "q59", "q60",
      "q61", "q62", "q63", "q64", "q65", "q66", "q67", "q68", "q69", "q70",
      "q71", "q72", "q73", "q74", "q75", "q76", "q77", "q78", "q79", "q80",
      "q81", "q82", "q83", "q84", "q85", "q86", "q87", "q88", "q89", "q90",
      "q91", "q92", "q93", "q94", "q95", "q96", "q97", "q98", "q99")

    // If `--query-filter` defined, filters the queries that this option selects
    val queriesToRun = if (benchmarkArgs.queryFilter.nonEmpty) {
      val queries = tpcdsQueries.filter { case queryName =>
        benchmarkArgs.queryFilter.contains(queryName)
      }
      if (queries.isEmpty) {
        throw new RuntimeException(
          s"Empty queries to run. Bad query name filter: ${benchmarkArgs.queryFilter}")
      }
      queries
    } else {
      tpcdsQueries
    }

    // create output directory
    if (Files.exists(outputDir)) {
      System.err.println(s"output dir already existed: $outputDir, cannot continue")
      System.exit(-1)
    }
    Files.createDirectories(outputDir)

    // start spark
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName)
    val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()

    // setup tables
    val tables: Seq[String] = Seq("catalog_page", "catalog_returns", "customer", "customer_address",
      "customer_demographics", "date_dim", "household_demographics", "inventory", "item",
      "promotion", "store", "store_returns", "catalog_sales", "web_sales", "store_sales",
      "web_returns", "web_site", "reason", "call_center", "warehouse", "ship_mode", "income_band",
      "time_dim", "web_page")
    tables.par.foreach { tableName =>
      spark.read.parquet(s"$dataLocation/$tableName").createOrReplaceTempView(tableName)
      tableName -> spark.table(tableName).count()
    }

    // run queries
    val logWriter = new PrintWriter(s"$outputDir/log")
    var numSucceeded = 0
    var numFailed = 0
    for (query <- queriesToRun) {
      logWriter.println(s"running case $query")
      logWriter.flush()

      val queryResource = s"tpcds/queries/$query.sql"
      val queryString = {
        val in = Thread.currentThread
          .getContextClassLoader
          .getResourceAsStream(queryResource)
        IOUtils.toString(in)
      }

      for (round <- 1 to benchmarkArgs.round) {
        spark.sparkContext.setJobDescription(s"case: $query, round: $round")
        var rows: Array[Row] = Array()
        var succeeded = true
        val startTimeMs = System.currentTimeMillis()
        try {
          val df = spark.sql(queryString)
          rows = df.collect()
        } catch {
          case e: Exception =>
            succeeded = false
            System.err.println(s"case: $query, round: $round, error: ${e.getMessage}")
            e.printStackTrace(System.err)
        }
        val timeCostMs = System.currentTimeMillis() - startTimeMs

        if (succeeded) {
          numSucceeded += 1
        } else {
          numFailed += 1
        }
        val numRows = rows.length

        // write output
        val outputWriter = new PrintWriter(s"$outputDir/$query-$round.out")
        for (row <- rows) {
          outputWriter.println(row.mkString("\t"))
        }
        outputWriter.close()

        logWriter.println(s"" +
          s"case: $query, " +
          s"round: $round, " +
          s"succeeded: $succeeded, " +
          s"rows: $numRows, " +
          f"time: ${timeCostMs * 1e-3}%1.3f sec")
        logWriter.flush()
      }
    }
    logWriter.println(s"total succeeded: $numSucceeded, total failed: $numFailed")
    logWriter.flush()
  }
}
