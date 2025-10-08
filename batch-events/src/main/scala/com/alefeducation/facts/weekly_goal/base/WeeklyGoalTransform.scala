package com.alefeducation.facts.weekly_goal.base

import com.alefeducation.base.SparkBatchService
import com.alefeducation.facts.weekly_goal.base.WeeklyGoalTransform._
import com.alefeducation.service.DataSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants._
import com.alefeducation.util.Helpers._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{lit, when}

import scala.collection.immutable.Map

class WeeklyGoalTransform(val session: SparkSession,
                          val service: SparkBatchService) {

  import session.implicits._

  private def getCombinedStarEarned: Option[DataFrame] = {
    val weeklyGoalStarEarnedDataFrame = service.readOptional(ParquetWeeklyGoalStarEarnedSource, session)
    val weeklyStagingStarEarnedDataFrame = service.readOptional(ParquetWeeklyGoalStarEarnedStagingSource, session)
    weeklyGoalStarEarnedDataFrame.unionOptionalByNameWithEmptyCheck(weeklyStagingStarEarnedDataFrame)
  }

  private def getCombinedWeeklyGoalCompleted(weeklyGoalDF: Option[DataFrame]): Option[DataFrame] = {
    val weeklyGoalCompletedDF = weeklyGoalDF.flatMap(_.filter($"eventType".isin(WeeklyGoalCompletedEvent)).checkEmptyDf)
    val weeklyGoalCompletedStagingParquet = service.readOptional(ParquetWeeklyGoalStagingSource, session)
    weeklyGoalCompletedDF.unionOptionalByNameWithEmptyCheck(weeklyGoalCompletedStagingParquet)
  }

  def transform(): Option[DataSink] = {

    val weeklyGoalParquet = service.readOptional(ParquetWeeklyGoalSource, session).map(_.cache())
    val weeklyGoalCreatedExpiredDF = weeklyGoalParquet
      .flatMap(_.filter($"eventType".isin(WeeklyGoalCreatedEvent, WeeklyGoalExpiredEvent))
        .checkEmptyDf
        .map(_.withColumn("stars", lit(-1))))
    val weeklyGoalCompletedWithStars = getCombinedWeeklyGoalCompleted(weeklyGoalParquet)
      .map(_.select("eventType", "weeklyGoalId", "goalId"))
      .joinOptionalStrict(getCombinedStarEarned.map(_.drop("eventType", "academicYearId")), Seq("weeklyGoalId"))
    val weeklyGoalAllEvents = weeklyGoalCreatedExpiredDF.unionOptionalByNameWithEmptyCheck(weeklyGoalCompletedWithStars)

    val uniqueIds = List("classId", "studentId", "goalId", "weeklyGoalId", "actionStatus")
    val weeklyGoalTransformed = weeklyGoalAllEvents.map(
      _.withColumn("actionStatus",
        when($"eventType" === WeeklyGoalCreatedEvent, lit(WeeklyGoalCreated))
          .otherwise(when($"eventType" === WeeklyGoalCompletedEvent, lit(WeeklyGoalCompleted))
            .otherwise(when($"eventType" === WeeklyGoalExpiredEvent, lit(WeeklyGoalExpired))
              .otherwise(lit(MinusOne))))
      )
        .transformForInsertFact(WeeklyGoalCols, FactWeeklyGoalEntity, ids = uniqueIds)
    )


    weeklyGoalTransformed.map(DataSink(ParquetWeeklyGoalTransformedSink, _))
  }
}

object WeeklyGoalTransform {

  val WeeklyGoalCreated = 1
  val WeeklyGoalCompleted = 2
  val WeeklyGoalExpired = 3

  val WeeklyGoalTransformService = "parquet-weekly-goal-transform"

  val ParquetWeeklyGoalSource = "parquet-weekly-goal-source"
  val ParquetWeeklyGoalStagingSource = "parquet-weekly-goal-staging-source"

  val ParquetWeeklyGoalStarEarnedSource = "parquet-weekly-goal-star-earned-source"
  val ParquetWeeklyGoalStarEarnedStagingSource = "parquet-weekly-goal-star-earned-staging-source"

  val ParquetWeeklyGoalTransformedSink = "parquet-weekly-goal-transformed-source"

  val session: SparkSession = SparkSessionUtils.getSession(WeeklyGoalTransformService)
  val service = new SparkBatchService(WeeklyGoalTransformService, session)

  lazy val WeeklyGoalCols: Map[String, String] = Map(
    "weeklyGoalId" -> "fwg_id",
    "studentId" -> "fwg_student_id",
    "tenantId" -> "fwg_tenant_id",
    "classId" -> "fwg_class_id",
    "goalId" -> "fwg_type_id",
    "actionStatus" -> "fwg_action_status",
    "stars" -> "fwg_star_earned",
    "eventDateDw" -> "fwg_date_dw_id",
    "occurredOn" -> "occurredOn"
  )

  def main(args: Array[String]): Unit = {
    val transformer = new WeeklyGoalTransform(session, service)
    service.run(transformer.transform())
  }
}
