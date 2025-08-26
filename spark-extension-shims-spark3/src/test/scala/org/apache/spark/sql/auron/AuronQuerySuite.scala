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
package org.apache.spark.sql.auron

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.Row

import org.apache.auron.util.AuronTestUtils

class AuronQuerySuite
    extends org.apache.spark.sql.QueryTest
    with BaseAuronSQLSuite
    with AuronSQLTestHelper {
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
          "insert into test_with_complex_type select 1 as id, map('zero', '0', 'one', '1') as m, array('test','auron') as l, 'auron' as s")
        checkAnswer(
          sql("select id,l,m from test_with_complex_type"),
          Seq(Row(1, ArrayBuffer("test", "auron"), Map("one" -> "1", "zero" -> "0"))))
      })
  }

  test("binary type in range partitioning") {
    withTable("t1", "t2") {
      sql("create table t1(c1 binary, c2 int) using parquet")
      sql("insert into t1 values (cast('test1' as binary), 1), (cast('test2' as binary), 2)")
      val df = sql("select c2 from t1 order by c1")
      checkAnswer(df, Seq(Row(1), Row(2)))
    }
  }

  test("repartition over MapType") {
    withTable("t_map") {
      sql("create table t_map using parquet as select map('a', '1', 'b', '2') as data_map")
      val df = sql("SELECT /*+ repartition(10) */ data_map FROM t_map")
      checkAnswer(df, Seq(Row(Map("a" -> "1", "b" -> "2"))))
    }
  }

  test("repartition over MapType with ArrayType") {
    withTable("t_map_struct") {
      sql(
        "create table t_map_struct using parquet as select named_struct('m', map('x', '1')) as data_struct")
      val df = sql("SELECT /*+ repartition(10) */ data_struct FROM t_map_struct")
      checkAnswer(df, Seq(Row(Row(Map("x" -> "1")))))
    }
  }

  test("repartition over ArrayType with MapType") {
    withTable("t_array_map") {
      sql("""
          |create table t_array_map using parquet as
          |select array(map('k1', 1, 'k2', 2), map('k3', 3)) as array_of_map
          |""".stripMargin)
      val df = sql("SELECT /*+ repartition(10) */ array_of_map FROM t_array_map")
      checkAnswer(df, Seq(Row(Seq(Map("k1" -> 1, "k2" -> 2), Map("k3" -> 3)))))
    }
  }

  test("repartition over StructType with MapType") {
    withTable("t_struct_map") {
      sql("""
          |create table t_struct_map using parquet as
          |select named_struct('id', 101, 'metrics', map('ctr', 0.123d, 'cvr', 0.045d)) as user_metrics
          |""".stripMargin)
      val df = sql("SELECT /*+ repartition(10) */ user_metrics FROM t_struct_map")
      checkAnswer(df, Seq(Row(Row(101, Map("ctr" -> 0.123, "cvr" -> 0.045)))))
    }
  }

  test("repartition over MapType with StructType") {
    withTable("t_map_struct_value") {
      sql("""
          |create table t_map_struct_value using parquet as
          |select map(
          |  'item1', named_struct('count', 3, 'score', 4.5d),
          |  'item2', named_struct('count', 7, 'score', 9.1d)
          |) as map_struct_value
          |""".stripMargin)
      val df = sql("SELECT /*+ repartition(10) */ map_struct_value FROM t_map_struct_value")
      checkAnswer(df, Seq(Row(Map("item1" -> Row(3, 4.5), "item2" -> Row(7, 9.1)))))
    }
  }

  test("repartition over nested MapType") {
    withTable("t_nested_map") {
      sql("""
          |create table t_nested_map using parquet as
          |select map(
          |  'outer1', map('inner1', 10, 'inner2', 20),
          |  'outer2', map('inner3', 30)
          |) as nested_map
          |""".stripMargin)
      val df = sql("SELECT /*+ repartition(10) */ nested_map FROM t_nested_map")
      checkAnswer(
        df,
        Seq(Row(
          Map("outer1" -> Map("inner1" -> 10, "inner2" -> 20), "outer2" -> Map("inner3" -> 30)))))
    }
  }

  test("repartition over ArrayType of StructType with MapType") {
    withTable("t_array_struct_map") {
      sql("""
          |create table t_array_struct_map using parquet as
          |select array(
          |  named_struct('name', 'user1', 'features', map('f1', 1.0d, 'f2', 2.0d)),
          |  named_struct('name', 'user2', 'features', map('f3', 3.5d))
          |) as user_feature_array
          |""".stripMargin)
      val df = sql("SELECT /*+ repartition(10) */ user_feature_array FROM t_array_struct_map")
      checkAnswer(
        df,
        Seq(
          Row(
            Seq(Row("user1", Map("f1" -> 1.0f, "f2" -> 2.0f)), Row("user2", Map("f3" -> 3.5f))))))
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
    if (AuronTestUtils.isSparkV32OrGreater) {
      Seq(true, false).foreach { forcePositionalEvolution =>
        withEnvConf(
          AuronConf.ORC_FORCE_POSITIONAL_EVOLUTION.key -> forcePositionalEvolution.toString) {
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
  }

  test("SPARK-32864: Support ORC forced positional evolution with partitioned table") {
    if (AuronTestUtils.isSparkV32OrGreater) {
      Seq(true, false).foreach { forcePositionalEvolution =>
        withEnvConf(
          AuronConf.ORC_FORCE_POSITIONAL_EVOLUTION.key -> forcePositionalEvolution.toString) {
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
                     |CREATE TABLE t(c3 INT, c2 INT)
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
}
