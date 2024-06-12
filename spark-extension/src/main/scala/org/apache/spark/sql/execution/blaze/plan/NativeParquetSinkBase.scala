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

import java.net.URI
import java.security.PrivilegedExceptionAction
import java.util
import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.ql.io.parquet.convert.HiveSchemaConverter
import org.apache.hadoop.hive.ql.io.IOConstants
import org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriteSupport
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.serde.serdeConstants
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils
import org.apache.hadoop.hive.serde2.SerDeUtils
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.blaze.JniBridge
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.blaze.MetricNode
import org.apache.spark.sql.blaze.NativeHelper
import org.apache.spark.sql.blaze.NativeRDD
import org.apache.spark.sql.blaze.NativeSupports
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.hive.blaze.HiveClientHelper
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.OneToOneDependency
import org.apache.spark.sql.execution.UnaryExecNode
import org.blaze.protobuf.ParquetProp
import org.blaze.protobuf.ParquetSinkExecNode
import org.blaze.protobuf.PhysicalPlanNode

abstract class NativeParquetSinkBase(
    sparkSession: SparkSession,
    table: CatalogTable,
    partition: Map[String, Option[String]],
    override val child: SparkPlan,
    override val metrics: Map[String, SQLMetric])
    extends UnaryExecNode
    with NativeSupports {

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def doExecuteNative(): NativeRDD = {
    val hiveQlTable = HiveClientHelper.toHiveTable(table)
    val tableDesc = new TableDesc(
      hiveQlTable.getInputFormatClass,
      hiveQlTable.getOutputFormatClass,
      hiveQlTable.getMetadata)
    val tableSchema = table.schema
    val hadoopConf = newHadoopConf(tableDesc)
    val job = new Job(hadoopConf)
    val parquetFileFormat = new ParquetFileFormat()
    parquetFileFormat.prepareWrite(sparkSession, job, Map(), tableSchema)

    val serializableConf = new SerializableConfiguration(job.getConfiguration)
    val numDynParts = partition.count(_._2.isEmpty)

    val inputRDD = NativeHelper.executeNative(child)
    val nativeMetrics = MetricNode(metrics, inputRDD.metrics :: Nil)
    val nativeDependencies = new OneToOneDependency(inputRDD) :: Nil
    new NativeRDD(
      sparkSession.sparkContext,
      nativeMetrics,
      inputRDD.partitions,
      nativeDependencies,
      inputRDD.isShuffleReadFull,
      (partition, context) => {

        // init hadoop fs
        val resourceId = s"NativeParquetSinkExec:${UUID.randomUUID().toString}"
        JniBridge.resourcesMap.put(
          resourceId,
          (location: String) => {
            NativeHelper.currentUser.doAs(new PrivilegedExceptionAction[FileSystem] {
              override def run(): FileSystem =
                FileSystem.get(new URI(location), serializableConf.value)
            })
          })

        // init parquet schema
        val job = new Job(new JobConf(serializableConf.value))
        val tableProperties = tableDesc.getProperties
        val columnNameProperty: String = tableProperties.getProperty(IOConstants.COLUMNS)
        val columnTypeProperty: String = tableProperties.getProperty(IOConstants.COLUMNS_TYPES)
        val columnNameDelimiter: String =
          if (tableProperties.containsKey(serdeConstants.COLUMN_NAME_DELIMITER)) {
            tableProperties.getProperty(serdeConstants.COLUMN_NAME_DELIMITER)
          } else {
            String.valueOf(SerDeUtils.COMMA)
          }
        var columnNames: util.List[String] = new util.ArrayList()
        var columnTypes: util.List[TypeInfo] = new util.ArrayList()
        if (columnNameProperty.nonEmpty) {
          columnNames = columnNameProperty.split(columnNameDelimiter).toList.asJava
        }
        if (columnTypeProperty.nonEmpty) {
          columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty)
        }
        val schema = HiveSchemaConverter.convert(columnNames, columnTypes)
        DataWritableWriteSupport.setSchema(schema, job.getConfiguration)

        // init parquet props
        val nativeProps = job.getConfiguration.asScala
          .filter(_.getKey.startsWith("parquet."))
          .map(entry =>
            ParquetProp
              .newBuilder()
              .setKey(entry.getKey)
              .setValue(entry.getValue)
              .build())

        val inputPartition = inputRDD.partitions(partition.index)
        val parquetSink = ParquetSinkExecNode
          .newBuilder()
          .setInput(inputRDD.nativePlan(inputPartition, context))
          .setFsResourceId(resourceId)
          .setNumDynParts(numDynParts)
          .addAllProp(nativeProps.asJava)
        PhysicalPlanNode.newBuilder().setParquetSink(parquetSink).build()
      },
      "ParquetSink")
  }

  protected def newHadoopConf(_tableDesc: TableDesc): Configuration =
    sparkSession.sessionState.newHadoopConf()
}
