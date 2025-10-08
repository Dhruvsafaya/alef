package com.alefeducation.facts.weekly_goal.base

import com.alefeducation.base.SparkBatchService
import com.alefeducation.facts.weekly_goal.base.WeeklyGoalTransform._
import com.alefeducation.service.DataSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession

class WeeklyGoalStaging(val session: SparkSession,
                        val service: SparkBatchService,
                        val source: String,
                        val eventType: String,
                        val stagingSource: String,
                        val sink: String
                       ) {
  val ParquetWeeklyGoalTransformedSource = "parquet-weekly-goal-transformed-source"

  import session.implicits._

  def transform(): Option[DataSink] = {
    val weeklyGoalSource = service
      .readOptional(source, session)
      .flatMap(_.filter($"eventType".isin(eventType)).checkEmptyDf)
    val weeklyGoalStagingSource = service.readOptional(stagingSource, session)
    val combinedWeeklyGoal = weeklyGoalSource.unionOptionalByNameWithEmptyCheck(weeklyGoalStagingSource)
    val transformedData = service.readOptional(ParquetWeeklyGoalTransformedSource, session)
    val completedWeeklyGoal = transformedData
      .flatMap(_.filter($"fwg_action_status" === WeeklyGoalCompleted)
        .checkEmptyDf.map(_.select($"fwg_id".as("weeklyGoalId"))))
    val unMappedData = combinedWeeklyGoal.findUnmapped(completedWeeklyGoal, Seq("weeklyGoalId"))
    unMappedData.map(_.toStagingSink(sink)(session))
  }
}

object WeeklyGoalCompletedStaging {
  val WeeklyGoalCompletedStaging = "weekly-goal-completed-staging"

  val ParquetWeeklyGoalSource = "parquet-weekly-goal-source"
  val ParquetWeeklyGoalStagingSource = "parquet-weekly-goal-staging-source"
  val ParquetWeeklyGoalStagingSink = "parquet-weekly-goal-staging-sink"

  val session: SparkSession = SparkSessionUtils.getSession(WeeklyGoalCompletedStaging)
  val service = new SparkBatchService(WeeklyGoalCompletedStaging, session)

  def main(args: Array[String]): Unit = {
    val transformer = new WeeklyGoalStaging(session,
      service,
      ParquetWeeklyGoalSource,
      WeeklyGoalCompletedEvent,
      ParquetWeeklyGoalStagingSource,
      ParquetWeeklyGoalStagingSink)
    service.run(transformer.transform())
  }
}

object WeeklyGoalStarEarnedStaging {
  val WeeklyGoalStarEarnedStaging = "weekly-goal-star-earned-staging"
  val ParquetWeeklyGoalStarEarnedSource = "parquet-weekly-goal-star-earned-source"
  val ParquetWeeklyGoalStarEarnedStagingSource = "parquet-weekly-goal-star-earned-staging-source"

  val ParquetWeeklyGoalStarEarnedStagingSink = "parquet-weekly-goal-star-earned-staging-sink"

  val session: SparkSession = SparkSessionUtils.getSession(WeeklyGoalStarEarnedStaging)
  val service = new SparkBatchService(WeeklyGoalStarEarnedStaging, session)

  def main(args: Array[String]): Unit = {
    val transformer = new WeeklyGoalStaging(session,
      service,
      ParquetWeeklyGoalStarEarnedSource,
      WeeklyGoalStarEarnedEvent,
      ParquetWeeklyGoalStarEarnedStagingSource,
      ParquetWeeklyGoalStarEarnedStagingSink)
    service.run(transformer.transform())
  }
}
