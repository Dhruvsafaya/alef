package com.alefeducation.facts.pathway.level.completed

import com.alefeducation.base.SparkBatchService
import com.alefeducation.util.{RedshiftBatchWriter, SparkSessionUtils}

object LevelCompletedRedshift {
  val LevelCompletedRedshiftService = "redshift-level-completed"
  val ParquetLevelCompletedTransformedSource = "level-completed-transformed-source"
  val RedshiftLevelCompletedSink = "redshift-level-completed-sink"

  val session = SparkSessionUtils.getSession(LevelCompletedRedshiftService)
  val service = new SparkBatchService(LevelCompletedRedshiftService, session)

  def main(args: Array[String]): Unit = {
    val writer = new RedshiftBatchWriter(session, service, List(ParquetLevelCompletedTransformedSource), RedshiftLevelCompletedSink)
    service.runAll(writer.write().flatten)
  }
}
