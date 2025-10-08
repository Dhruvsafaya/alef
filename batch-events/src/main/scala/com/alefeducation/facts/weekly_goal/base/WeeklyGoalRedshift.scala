package com.alefeducation.facts.weekly_goal.base

import com.alefeducation.base.SparkBatchService
import com.alefeducation.facts.pathway.level.completed.LevelCompletedRedshift.service
import com.alefeducation.util.{RedshiftBatchWriter, SparkSessionUtils}

object WeeklyGoalRedshift {

  val WeeklyGoalRedshiftService = "redshift-weekly-goal"
  val ParquetWeeklyGoalTransformedSource = "parquet-weekly-goal-transformed-source"
  val RedshiftWeeklyGoalSink = "redshift-weekly-goal-sink"

  val session = SparkSessionUtils.getSession(WeeklyGoalRedshiftService)
  val service = new SparkBatchService(WeeklyGoalRedshiftService, session)
  def main(args: Array[String]): Unit = {
    val writer = new RedshiftBatchWriter(session, service, List(ParquetWeeklyGoalTransformedSource), RedshiftWeeklyGoalSink)
    service.runAll(writer.write().flatten)
  }
}
