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

import org.apache.spark.sql.blaze.shims.BroadcastShimsImpl
import org.apache.spark.sql.blaze.shims.ExprShimsImpl
import org.apache.spark.sql.blaze.shims.RDDShimsImpl
import org.apache.spark.sql.blaze.shims.ShuffleShimsImpl
import org.apache.spark.sql.blaze.shims.SparkPlanShimsImpl

class Shims303Impl extends Shims {
  override val rddShims: RDDShims = new RDDShimsImpl
  override val sparkPlanShims: SparkPlanShims = new SparkPlanShimsImpl
  override val shuffleShims: ShuffleShims = new ShuffleShimsImpl
  override val broadcastShims: BroadcastShims = new BroadcastShimsImpl
  override val exprShims: ExprShims = new ExprShimsImpl
}
