package com.alefeducation.facts.pathway.level.completed

import com.alefeducation.base.SparkBatchService
import com.alefeducation.util.{DeltaBatchWriter, SparkSessionUtils}

object LevelCompletedDelta {
  val LevelCompletedService = "delta-level-completed"
  val ParquetLevelCompletedTransformedSource = "level-completed-transformed-source"
  val DeltaLevelCompletedSink = "delta-level-completed-sink"

  val session = SparkSessionUtils.getSession(LevelCompletedService)
  val service = new SparkBatchService(LevelCompletedService, session)

  def main(args: Array[String]): Unit = {
    val writer = new DeltaBatchWriter(
      session, service, List(ParquetLevelCompletedTransformedSource), DeltaLevelCompletedSink, isFact = true
    )
    service.runAll(writer.write().flatten)
  }
}
