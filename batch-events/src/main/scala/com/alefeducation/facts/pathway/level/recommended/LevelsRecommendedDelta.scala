package com.alefeducation.facts.pathway.level.recommended

import com.alefeducation.base.SparkBatchService
import com.alefeducation.util.{DeltaBatchWriter, SparkSessionUtils}

object LevelsRecommendedDelta {
  val LevelsRecommendedService = "delta-levels-recommended"
  val ParquetLevelsRecommendedTransformedSource = "levels-recommended-transformed-source"
  val DeltaLevelsRecommendedSink = "delta-levels-recommended-sink"

  val session = SparkSessionUtils.getSession(LevelsRecommendedService)
  val service = new SparkBatchService(LevelsRecommendedService, session)

  def main(args: Array[String]): Unit = {
    val writer = new DeltaBatchWriter(
      session, service, List(ParquetLevelsRecommendedTransformedSource), DeltaLevelsRecommendedSink, isFact = true
    )
    service.runAll(writer.write().flatten)
  }
}
