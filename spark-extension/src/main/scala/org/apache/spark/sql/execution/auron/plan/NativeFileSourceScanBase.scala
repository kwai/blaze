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
package org.apache.spark.sql.execution.auron.plan

import java.net.URI
import java.security.PrivilegedExceptionAction

import scala.collection.JavaConverters._

import org.apache.commons.lang3.reflect.MethodUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.MapPartitionsRDD
import org.apache.spark.sql.auron.JniBridge
import org.apache.spark.sql.auron.NativeConverters
import org.apache.spark.sql.auron.NativeHelper
import org.apache.spark.sql.auron.NativeSupports
import org.apache.spark.sql.auron.Shims
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.datasources.FileScanRDD
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{DecimalType, NullType, StructField, StructType}
import org.apache.spark.util.SerializableConfiguration

import org.apache.auron.{protobuf => pb}

abstract class NativeFileSourceScanBase(basedFileScan: FileSourceScanExec)
    extends LeafExecNode
    with NativeSupports {

  override lazy val metrics: Map[String, SQLMetric] =
    NativeHelper.getNativeFileScanMetrics(sparkContext)

  override val output: Seq[Attribute] = basedFileScan.output
  override val outputPartitioning: Partitioning = basedFileScan.outputPartitioning

  protected val inputFileScanRDD: FileScanRDD = {
    MethodUtils.invokeMethod(basedFileScan, true, "prepare")
    MethodUtils.invokeMethod(basedFileScan, true, "waitForSubqueries")
    basedFileScan.inputRDDs().head match {
      case rdd: FileScanRDD => rdd
      case rdd: MapPartitionsRDD[_, _] => rdd.prev.asInstanceOf[FileScanRDD]
    }
  }

  private val partitionSchema = basedFileScan.relation.partitionSchema

  private val fileSizes = inputFileScanRDD.filePartitions
    .flatMap(_.files)
    .groupBy(_.filePath)
    .mapValues(_.foldLeft(0L)(_ + _.length))
    .map(identity) // make this map serializable
    .toMap

  // predicate pruning is buggy for decimal type, so we need to
  // temporarily disable predicate pruning for decimal type
  // see https://github.com/kwai/auron/issues/1032
  protected def nativePruningPredicateFilters: Seq[pb.PhysicalExprNode] =
    basedFileScan.dataFilters
      .filter(expr => expr.find(_.dataType.isInstanceOf[DecimalType]).isEmpty)
      .map(expr => NativeConverters.convertScanPruningExpr(expr))

  protected def nativeFileSchema: pb.Schema =
    NativeConverters.convertSchema(StructType(basedFileScan.relation.dataSchema.map {
      case field if basedFileScan.requiredSchema.exists(_.name == field.name) =>
        field.copy(nullable = true)
      case field =>
        // avoid converting unsupported type in non-used fields
        StructField(field.name, NullType, nullable = true)
    }))

  protected def nativePartitionSchema: pb.Schema =
    NativeConverters.convertSchema(partitionSchema)

  protected def nativeFileGroups: FilePartition => pb.FileGroup = (partition: FilePartition) => {
    // list input file statuses
    val nativePartitionedFile = (file: PartitionedFile) => {
      val nativePartitionValues = partitionSchema.zipWithIndex.map { case (field, index) =>
        NativeConverters
          .convertExpr(Literal(file.partitionValues.get(index, field.dataType), field.dataType))
          .getLiteral
      }
      pb.PartitionedFile
        .newBuilder()
        .setPath(s"${file.filePath}")
        .setSize(fileSizes(file.filePath))
        .addAllPartitionValues(nativePartitionValues.asJava)
        .setLastModifiedNs(0)
        .setRange(
          pb.FileRange
            .newBuilder()
            .setStart(file.start)
            .setEnd(file.start + file.length)
            .build())
        .build()
    }
    pb.FileGroup
      .newBuilder()
      .addAllFiles(partition.files.map(nativePartitionedFile).toList.asJava)
      .build()
  }

  // check whether native converting is supported
  nativePruningPredicateFilters
  nativeFileSchema
  nativePartitionSchema
  nativeFileGroups

  protected def putJniBridgeResource(
      resourceId: String,
      broadcastedHadoopConf: Broadcast[SerializableConfiguration]): Unit = {
    val sharedConf = broadcastedHadoopConf.value.value
    JniBridge.resourcesMap.put(
      resourceId,
      (location: String) => {
        val getFsTimeMetric = metrics("io_time_getfs")
        val currentTimeMillis = System.currentTimeMillis()
        val fs = NativeHelper.currentUser.doAs(new PrivilegedExceptionAction[FileSystem] {
          override def run(): FileSystem = {
            FileSystem.get(new URI(location), sharedConf)
          }
        })
        getFsTimeMetric.add((System.currentTimeMillis() - currentTimeMillis) * 1000000)
        fs
      })
  }

  protected def broadcastedHadoopConf: Broadcast[SerializableConfiguration] = {
    val sparkSession = Shims.get.getSqlContext(basedFileScan).sparkSession
    val hadoopConf =
      sparkSession.sessionState.newHadoopConfWithOptions(basedFileScan.relation.options)
    sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
  }

  override protected def doCanonicalize(): SparkPlan = basedFileScan.canonicalized
}
