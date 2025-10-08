package com.alefeducation.util

import com.alefeducation.io.UnityCatalog
import com.alefeducation.util.Resources.{getBatchExecutionLogS3Path, getBatchExecutionLogTable}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

class BatchExecutionLogger(private val spark: SparkSession, private val unity: UnityCatalog) {

  def getPreviousOffset(batchJobId: String, source: String): Offset = {

    import spark.implicits._
    val executionLog: BatchExecutionLog =
      unity
        .readOptional(getBatchExecutionLogS3Path).flatMap(
        _.where($"batch_job_id" === batchJobId && $"source" === source && ($"status" === "SUCCESS"))
          .as[BatchExecutionLog]
          .orderBy($"created_at".desc)
          .collect()
          .headOption
      ).getOrElse(BatchExecutionLog(batch_job_id = batchJobId, source = "", start_offset = 0L, end_offset = 0L, status = ""))

    Offset(executionLog.start_offset, executionLog.end_offset)
  }

  def log(status: String, batchJobId: String, source: String, offset: Offset): Unit = {
    val finishedExecutionLog: DataFrame =
      spark.createDataFrame(Seq(new BatchExecutionLog(batchJobId, source, offset.start, offset.end, status))).toDF()
    unity.write(finishedExecutionLog, getBatchExecutionLogS3Path, getBatchExecutionLogS3Path, List("batch_job_id"))
  }

  def getOffsets(df: DataFrame): Option[Offset] =
    if (!df.isEmpty) {
      val offsetColumnName = "_load_time"
      val sortedDF = df.select(col(offsetColumnName)).distinct().sort(offsetColumnName)
      val startOffset = sortedDF.head().getAs[Long](offsetColumnName)
      val endOffset = sortedDF.tail(1).head.getAs[Long](offsetColumnName)

      Some(Offset(startOffset, endOffset))
    } else None
}

case class Offset(start: Long, end: Long) {
  def hasMoved(old: Offset): Boolean = {
    old.end < this.start && this.start <= this.end
  }
}

case class BatchExecutionLog(batch_job_id: String, source: String, start_offset: Long, end_offset: Long, status: String)
