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

class BlazeFunctionSuite extends org.apache.spark.sql.QueryTest with BaseBlazeSQLSuite {

  test("sum function with float input") {
    withTable("t1") {
      withSQLConf("spark.blaze.enable" -> "false") {
        sql("set spark.blaze.enable=false")
        sql("create table t1 using parquet as select 1.0f as c1")
        val df = sql("select sum(c1) from t1")
        checkAnswer(df, Seq(Row(1.23, 1.1)))
      }
    }
  }
}
