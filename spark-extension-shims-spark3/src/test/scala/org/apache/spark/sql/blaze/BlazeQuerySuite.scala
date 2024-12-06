package org.apache.spark.sql.blaze

import org.apache.spark.sql.Row

class BlazeQuerySuite extends org.apache.spark.sql.QueryTest with BaseBlazeSQLSuite {

  test("test partition path has url encoded character") {
    withTable("t1") {
      sql("create table t1 using parquet PARTITIONED BY (part) as select 1 as c1, 2 as c2, 'test test' as part")
      val df = sql("select * from t1")
      checkAnswer(df, Seq(Row(1, 2, "test test")))
    }
  }

}
