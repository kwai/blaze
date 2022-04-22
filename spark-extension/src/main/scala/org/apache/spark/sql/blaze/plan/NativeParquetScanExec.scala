package org.apache.spark.sql.blaze.plan

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.blaze.{
  JniBridge,
  MetricNode,
  NativeConverters,
  NativeRDD,
  NativeSupports
}
import org.apache.spark.sql.catalyst.expressions.{And, Attribute}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.datasources.FileScanRDD
import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.metric.SQLMetric
import org.blaze.protobuf.FileGroup
import org.blaze.protobuf.FileScanExecConf
import org.blaze.protobuf.ParquetScanExecNode
import org.blaze.protobuf.PartitionedFile
import org.blaze.protobuf.PhysicalPlanNode
import org.blaze.protobuf.FileRange
import org.blaze.protobuf.Statistics

import scala.collection.JavaConverters._

import org.blaze.protobuf.LogicalExprNode

case class NativeParquetScanExec(basedFileScan: FileSourceScanExec)
    extends LeafExecNode
    with NativeSupports {

  override lazy val metrics: Map[String, SQLMetric] =
    NativeSupports.getDefaultNativeMetrics(sparkContext)

  override def output: Seq[Attribute] = basedFileScan.output
  override def outputPartitioning: Partitioning = basedFileScan.outputPartitioning

  private val nativeFileSchema = NativeConverters.convertSchema(basedFileScan.relation.schema)
  private val nativePruningPredicateFilter = basedFileScan.dataFilters
    .reduceOption(And)
    .map(NativeConverters.convertExprLogical)

  override def doExecute(): RDD[InternalRow] = doExecuteNative()
  override def doExecuteNative(): NativeRDD = {
    val inputFileScanRDD = basedFileScan.inputRDD.asInstanceOf[FileScanRDD]
    val partitions = inputFileScanRDD.filePartitions.toArray
    val nativeMetrics = MetricNode(
      Map(
        "output_rows" -> metrics("numOutputRows"),
        "blaze_output_ipc_rows" -> metrics("blazeExecIPCWrittenRows"),
        "blaze_output_ipc_bytes" -> metrics("blazeExecIPCWrittenBytes"),
        "blaze_exec_time" -> metrics("blazeExecTime")),
      Nil)

    val fileSchema = basedFileScan.relation.schema
    val outputSchema = basedFileScan.requiredSchema.fields
    val projection = outputSchema.map(field => fileSchema.fieldIndex(field.name))

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      partitions.asInstanceOf[Array[Partition]],
      Nil,
      (p, _) => {
        val nativeParquetScanConfBuilder = FileScanExecConf
          .newBuilder()
          .setStatistics(Statistics.getDefaultInstance)
          .setSchema(nativeFileSchema)
          .addAllProjection(projection.map(Integer.valueOf).toSeq.asJava)

        partitions.zipWithIndex.foreach {
          case (filePartition, i) =>
            val nativeFileGroupBuilder = FileGroup.newBuilder()
            if (i == p.index) {
              var fs: FileSystem = null
              filePartition.files.foreach {
                file =>
                  if (fs == null) {
                    fs = JniBridge.getHDFSFileSystem(file.filePath)
                  }
                  val fileStatus = fs.getFileStatus(new Path(file.filePath))
                  val range = FileRange
                    .newBuilder()
                    .setStart(file.start)
                    .setEnd(file.start + file.length)
                    .build()
                  nativeFileGroupBuilder.addFiles(
                    PartitionedFile
                      .newBuilder()
                      .setPath(file.filePath.replaceFirst("^file://", ""))
                      .setSize(fileStatus.getLen)
                      .setLastModifiedNs(fileStatus.getModificationTime * 1000 * 1000)
                      .setRange(range)
                      .build())
              }
            } else {
              filePartition.files.foreach { file =>
                val range = FileRange
                  .newBuilder()
                  .setStart(file.start)
                  .setEnd(file.start + file.length)
                  .build();
                nativeFileGroupBuilder.addFiles(
                  PartitionedFile
                    .newBuilder()
                    .setPath(file.filePath.replaceFirst("^file://", ""))
                    .setSize(file.length)
                    .setLastModifiedNs(0)
                    .setRange(range)
                    .build())
              }
            }
            nativeParquetScanConfBuilder.addFileGroups(nativeFileGroupBuilder.build())
        }

        val nativeParquetScanExecBuilder = ParquetScanExecNode
          .newBuilder()
          .setBaseConf(nativeParquetScanConfBuilder.build())
        if (nativePruningPredicateFilter.isDefined) {
          nativeParquetScanExecBuilder.setPruningPredicate(nativePruningPredicateFilter.get)
        }
        PhysicalPlanNode.newBuilder().setParquetScan(nativeParquetScanExecBuilder.build()).build()
      })
  }

  override val nodeName: String =
    s"NativeParquetScan ${basedFileScan.tableIdentifier.map(_.unquotedString).getOrElse("")}"

  override def simpleString(maxFields: Int): String =
    s"$nodeName (${basedFileScan.simpleString(maxFields)})"
}
