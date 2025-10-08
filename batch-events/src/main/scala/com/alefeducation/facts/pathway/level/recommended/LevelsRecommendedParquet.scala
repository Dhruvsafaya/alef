package com.alefeducation.facts.pathway.level.recommended

import com.alefeducation.base.SparkBatchService
import com.alefeducation.util.{ParquetBatchWriter, SparkSessionUtils}

object LevelsRecommendedParquet {
  val LevelsRecommendedParquetService = "parquet-levels-recommended"
  val LevelsRecommendedParquetSource = "parquet-levels-recommended-source"
  val LevelsRecommendedParquetSink = "parquet-levels-recommended-sink"
  val session = SparkSessionUtils.getSession(LevelsRecommendedParquetService)
  val service = new SparkBatchService(LevelsRecommendedParquetService, session)

  def main(args: Array[String]): Unit = {
    val writer = new ParquetBatchWriter(session, service, LevelsRecommendedParquetSource, LevelsRecommendedParquetSink)
    service.run(writer.write())
  }
}
