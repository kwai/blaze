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

import org.apache.spark.SparkConf
import org.apache.spark.sql.test.SharedSparkSession

trait BaseBlazeSQLSuite extends SharedSparkSession {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.extensions", "org.apache.spark.sql.blaze.BlazeSparkSessionExtension")
      .set(
        "spark.shuffle.manager",
        "org.apache.spark.sql.execution.blaze.shuffle.BlazeShuffleManager")
      .set("spark.memory.offHeap.enabled", "false")
      .set("spark.blaze.enable", "true")
  }

}
