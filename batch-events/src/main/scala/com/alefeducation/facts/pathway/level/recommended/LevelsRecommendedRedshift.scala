package com.alefeducation.facts.pathway.level.recommended

import com.alefeducation.base.SparkBatchService
import com.alefeducation.util.{RedshiftBatchWriter, SparkSessionUtils}

object LevelsRecommendedRedshift {
  val LevelsRecommendedRedshiftService = "redshift-levels-recommended"
  val ParquetLevelsRecommendedTransformedSource = "levels-recommended-transformed-source"
  val RedshiftLevelsRecommendedSink = "redshift-levels-recommended-sink"

  val session = SparkSessionUtils.getSession(LevelsRecommendedRedshiftService)
  val service = new SparkBatchService(LevelsRecommendedRedshiftService, session)

  def main(args: Array[String]): Unit = {
    val writer = new RedshiftBatchWriter(session, service, List(ParquetLevelsRecommendedTransformedSource), RedshiftLevelsRecommendedSink)
    service.runAll(writer.write().flatten)
  }
}
