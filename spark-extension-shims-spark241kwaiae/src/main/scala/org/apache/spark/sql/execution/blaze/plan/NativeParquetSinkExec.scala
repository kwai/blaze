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
package org.apache.spark.sql.execution.blaze.plan

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.io.compress.ZStandardCodec
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.hive.execution.HiveOptions

case class NativeParquetSinkExec(
    sparkSession: SparkSession,
    table: CatalogTable,
    partition: Map[String, Option[String]],
    override val child: SparkPlan,
    override val metrics: Map[String, SQLMetric])
    extends NativeParquetSinkBase(sparkSession, table, partition, child, metrics) {

  override def withNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
    copy(child = newChildren.head)

  override protected def newHadoopConf(tableDesc: TableDesc): Configuration = {
    val hadoopConf = super.newHadoopConf(tableDesc)

    // Set compression by priority
    HiveOptions
      .getHiveWriteCompression(tableDesc, sparkSession.sessionState.conf)
      .foreach { case (compression, codec) =>
        hadoopConf.set(compression, codec)
        if (codec == CompressionCodecName.ZSTD.name()) {
          val defaultLevel = ZStandardCodec.getCompressionLevel(hadoopConf)
          val hiveFileLevel = hadoopConf
            .getInt("spark.kwai.hivefile.compression.zstd.level", defaultLevel)
            .min(10) // do not use zstd level > 10
          hadoopConf.setInt("parquet.compression.codec.zstd.level", hiveFileLevel)
        }
      }
    hadoopConf
  }
}
