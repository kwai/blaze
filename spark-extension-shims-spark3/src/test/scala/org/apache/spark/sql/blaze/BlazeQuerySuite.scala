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
package org.apache.spark.sql.blaze

import org.apache.spark.sql.Row

import scala.collection.mutable.ArrayBuffer

class BlazeQuerySuite
    extends org.apache.spark.sql.QueryTest
    with BaseBlazeSQLSuite
    with BlazeSQLTestHelper {
  import testImplicits._

  test("test partition path has url encoded character") {
    withTable("t1") {
      sql(
        "create table t1 using parquet PARTITIONED BY (part) as select 1 as c1, 2 as c2, 'test test' as part")
      val df = sql("select * from t1")
      checkAnswer(df, Seq(Row(1, 2, "test test")))
    }
  }

  test("empty output in bnlj") {
    withTable("t1", "t2") {
      sql("create table t1 using parquet as select 1 as c1, 2 as c2")
      sql("create table t2 using parquet as select 1 as c1, 3 as c3")
      val df = sql("select 1 from t1 left join t2")
      checkAnswer(df, Seq(Row(1)))
    }
  }

  test("test filter with year function") {
    withTable("t1") {
      sql("create table t1 using parquet as select '2024-12-18' as event_time")
      checkAnswer(
        sql("""
            |select year, count(*)
            |from (select event_time, year(event_time) as year from t1) t
            |where year <= 2024
            |group by year
            |""".stripMargin),
        Seq(Row(2024, 1)))
    }
  }

  test("test select multiple spark ext functions with the same signature") {
    withTable("t1") {
      sql("create table t1 using parquet as select '2024-12-18' as event_time")
      checkAnswer(sql("select year(event_time), month(event_time) from t1"), Seq(Row(2024, 12)))
    }
  }

  test("test parquet/orc format table with complex data type") {
    def createTableStatement(format: String): String = {
      s"""create table test_with_complex_type(
         |id bigint comment 'pk',
         |m map<string, string> comment 'test read map type',
         |l array<string> comment 'test read list type',
         |s string comment 'string type'
         |) USING $format
         |""".stripMargin
    }
    Seq("parquet", "orc").foreach(format =>
      withTable("test_with_complex_type") {
        sql(createTableStatement(format))
        sql(
          "insert into test_with_complex_type select 1 as id, map('zero', '0', 'one', '1') as m, array('test','blaze') as l, 'blaze' as s")
        checkAnswer(
          sql("select id,l,m from test_with_complex_type"),
          Seq(Row(1, ArrayBuffer("test", "blaze"), Map("one" -> "1", "zero" -> "0"))))
      })
  }

  test("binary type in range partitioning") {
    withTable("t1", "t2") {
      sql("create table t1(c1 binary, c2 int) using parquet")
      sql(
        "insert into t1 values (to_binary('test1', 'utf-8'), 1), (to_binary('test2', 'utf-8'), 2)")
      val df = sql("select c2 from t1 order by c1")
      checkAnswer(df, Seq(Row(1), Row(2)))
    }
  }

  test("log function with negative input") {
    withTable("t1") {
      sql("create table t1 using parquet as select -1 as c1")
      val df = sql("select ln(c1) from t1")
      checkAnswer(df, Seq(Row(null)))
    }
  }

  test("floor function with long input") {
    withTable("t1") {
      sql("create table t1 using parquet as select 1L as c1, 2.2 as c2")
      val df = sql("select floor(c1), floor(c2) from t1")
      checkAnswer(df, Seq(Row(1, 2)))
    }
  }

  test("SPARK-32234 read ORC table with column names all starting with '_col'") {
    withTable("test_hive_orc_impl") {
      spark.sql(s"""
           | CREATE TABLE test_hive_orc_impl
           | (_col1 INT, _col2 STRING, _col3 INT)
           | USING ORC
               """.stripMargin)
      spark.sql(s"""
           | INSERT INTO
           | test_hive_orc_impl
           | VALUES(9, '12', 2020)
               """.stripMargin)

      val df = spark.sql("SELECT _col2 FROM test_hive_orc_impl")
      checkAnswer(df, Row("12"))
    }
  }

  test("SPARK-32864: Support ORC forced positional evolution") {
    Seq(true, false).foreach { forcePositionalEvolution =>
      withEnvConf(
        BlazeConf.ORC_FORCE_POSITIONAL_EVOLUTION.key -> forcePositionalEvolution.toString) {
        withTempPath { f =>
          val path = f.getCanonicalPath
          Seq[(Integer, Integer)]((1, 2), (3, 4), (5, 6), (null, null))
            .toDF("c1", "c2")
            .write
            .orc(path)
          val correctAnswer = Seq(Row(1, 2), Row(3, 4), Row(5, 6), Row(null, null))
          checkAnswer(spark.read.orc(path), correctAnswer)

          withTable("t") {
            sql(s"CREATE EXTERNAL TABLE t(c3 INT, c2 INT) USING ORC LOCATION '$path'")

            val expected = if (forcePositionalEvolution) {
              correctAnswer
            } else {
              Seq(Row(null, 2), Row(null, 4), Row(null, 6), Row(null, null))
            }

            checkAnswer(spark.table("t"), expected)
          }
        }
      }
    }
  }

  test("SPARK-32864: Support ORC forced positional evolution with partitioned table") {
    Seq(true, false).foreach { forcePositionalEvolution =>
      withEnvConf(
        BlazeConf.ORC_FORCE_POSITIONAL_EVOLUTION.key -> forcePositionalEvolution.toString) {
        withTempPath { f =>
          val path = f.getCanonicalPath
          Seq[(Integer, Integer, Integer)]((1, 2, 1), (3, 4, 2), (5, 6, 3), (null, null, 4))
            .toDF("c1", "c2", "p")
            .write
            .partitionBy("p")
            .orc(path)
          val correctAnswer = Seq(Row(1, 2, 1), Row(3, 4, 2), Row(5, 6, 3), Row(null, null, 4))
          checkAnswer(spark.read.orc(path), correctAnswer)

          withTable("t") {
            sql(s"""
                 |CREATE EXTERNAL TABLE t(c3 INT, c2 INT)
                 |USING ORC
                 |PARTITIONED BY (p int)
                 |LOCATION '$path'
                 |""".stripMargin)
            sql("MSCK REPAIR TABLE t")
            val expected = if (forcePositionalEvolution) {
              correctAnswer
            } else {
              Seq(Row(null, 2, 1), Row(null, 4, 2), Row(null, 6, 3), Row(null, null, 4))
            }

            checkAnswer(spark.table("t"), expected)
          }
        }
      }
    }
  }
}
