package org.apache.spark.sql.blaze.shims

import java.io.File

import org.apache.spark.sql.blaze.ShuffleShims
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.blaze.plan.ArrowShuffleExchangeBase
import org.apache.spark.sql.execution.blaze.plan.ArrowShuffleExchangeExec
import org.apache.spark.storage.FileSegment

class ShuffleShimsImpl extends ShuffleShims {
  override def createArrowShuffleExchange(
    outputPartitioning: Partitioning,
    child: SparkPlan
  ): ArrowShuffleExchangeBase = ArrowShuffleExchangeExec(outputPartitioning, child)

  override def createFileSegment(
    file: File,
    offset: Long,
    length: Long,
    numRecords: Long): FileSegment = new FileSegment(file, offset, length, numRecords)
}
