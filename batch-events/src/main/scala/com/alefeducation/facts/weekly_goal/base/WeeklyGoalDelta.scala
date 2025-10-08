package com.alefeducation.facts.weekly_goal.base

import com.alefeducation.base.SparkBatchService
import com.alefeducation.util.{DeltaBatchWriter, SparkSessionUtils}

object WeeklyGoalDelta {

  val WeeklyGoalDeltaService = "delta-weekly-goal"
  val ParquetWeeklyGoalTransformedSource = "parquet-weekly-goal-transformed-source"
  val DeltaWeeklyGoalSink = "delta-weekly-goal-sink"

  val session = SparkSessionUtils.getSession(WeeklyGoalDeltaService)
  val service = new SparkBatchService(WeeklyGoalDeltaService, session)

  def main(args: Array[String]): Unit = {
    val writer = new DeltaBatchWriter(
      session, service, List(ParquetWeeklyGoalTransformedSource), DeltaWeeklyGoalSink, isFact = true
    )
    service.runAll(writer.write().flatten)
  }
}
