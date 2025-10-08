package com.alefeducation.facts.weekly_goal.base

import com.alefeducation.base.SparkBatchService
import com.alefeducation.util.{ParquetBatchWriter, SparkSessionUtils}

object WeeklyGoalStarEarnedDataSink {
  val WeeklyGoalStarEarnedDataService = "parquet-weekly-goal-star-earned-data"

  val ParquetWeeklyGoalStarEarnedSource = "parquet-weekly-goal-star-earned-source"
  val WeeklyGoalStarEarnedDataSink = "weekly-goal-star-earned-data-sink"

  val session = SparkSessionUtils.getSession(WeeklyGoalStarEarnedDataService)
  val service = new SparkBatchService(WeeklyGoalStarEarnedDataService, session)

  def main(args: Array[String]): Unit = {
    val writer = new ParquetBatchWriter(session, service, ParquetWeeklyGoalStarEarnedSource, WeeklyGoalStarEarnedDataSink)
    service.run(writer.write())
  }
}
