package com.alefeducation.facts.weekly_goal.base

import com.alefeducation.base.SparkBatchService
import com.alefeducation.util.{ParquetBatchWriter, SparkSessionUtils}

object WeeklyGoalDataSink {
  val WeeklyGoalDataService = "weekly-goal-data"

  val ParquetWeeklyGoalSource = "parquet-weekly-goal-source"
  val WeeklyGoalDataSink = "weekly-goal-data-sink"

  val session = SparkSessionUtils.getSession(WeeklyGoalDataService)
  val service = new SparkBatchService(WeeklyGoalDataService, session)

  def main(args: Array[String]): Unit = {
    val writer = new ParquetBatchWriter(session, service, ParquetWeeklyGoalSource, WeeklyGoalDataSink)
    service.run(writer.write())
  }
}
