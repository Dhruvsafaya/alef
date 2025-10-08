package com.alefeducation.facts.pathway.level.completed

import com.alefeducation.base.SparkBatchService
import com.alefeducation.util.{ParquetBatchWriter, SparkSessionUtils}

object LevelCompletedParquet {
  val LevelCompletedParquetService = "parquet-level-completed"
  val LevelCompletedParquetSource = "parquet-level-completed-source"
  val LevelCompletedParquetSink = "parquet-level-completed-sink"
  val session = SparkSessionUtils.getSession(LevelCompletedParquetService)
  val service = new SparkBatchService(LevelCompletedParquetService, session)

  def main(args: Array[String]): Unit = {
    val writer = new ParquetBatchWriter(session, service, LevelCompletedParquetSource, LevelCompletedParquetSink)
    service.run(writer.write())
  }
}
