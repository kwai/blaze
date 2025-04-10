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
package org.apache.spark.sql.execution.blaze.shuffle

import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.sql.blaze.Shims

class BlazeRssShuffleWriter[K, V](metrics: ShuffleWriteMetricsReporter)
    extends BlazeRssShuffleWriterBase[K, V](metrics) {

  var rssShuffleWriterObject: RssPartitionWriterBase = _

  override def getRssPartitionWriter(
      handle: ShuffleHandle,
      mapId: Int,
      metrics: ShuffleWriteMetricsReporter,
      numPartitions: Int): RssPartitionWriterBase = {

    rssShuffleWriterObject = Shims.get
      .getRssPartitionWriter(handle, mapId, metrics, numPartitions)
      .get
    rssShuffleWriterObject
  }

  override def rssStop(success: Boolean): Unit = {
    if (rssShuffleWriterObject != null) {
      val time = System.currentTimeMillis()
      rssShuffleWriterObject.close(success)
      rssShuffleWriterObject = null
      metrics.incWriteTime(System.currentTimeMillis() - time)
    }
  }
}
