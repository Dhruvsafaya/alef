package com.alefeducation.facts.weekly_goal.activities

import com.alefeducation.base.SparkBatchService
import com.alefeducation.facts.weekly_goal.activities.WeeklyGoalActivityTransform._
import com.alefeducation.service.DataSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Helpers._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lit, when}

import scala.collection.immutable.Map

class WeeklyGoalActivityTransform(val session: SparkSession,
                                  val service: SparkBatchService) {

  import session.implicits._

  def transform(): Option[DataSink] = {
    val weeklyGoalActivityParquet = service.readOptional(ParquetWeeklyGoalActivitySource, session)

    val weeklyGoalTransformed = weeklyGoalActivityParquet.map(
      _.transform(
        _.withColumn("goalStatus",
          when($"status" === "Ongoing", lit(1)).otherwise(
            when($"status" === "Completed", lit(2))
          )
        )
      ).transformForInsertFact(WeeklyGoalActivityCols, FactWeeklyGoalActivityEntity))

    weeklyGoalTransformed.map(df => {
      DataSink(ParquetWeeklyGoalActivityTransformedSink, df)
    })
  }

}

object WeeklyGoalActivityTransform {

  val WeeklyGoalActivityTransformService = "transform-weekly-goal-activity"
  val ParquetWeeklyGoalActivitySource = "parquet-weekly-goal-activity-source"
  val ParquetWeeklyGoalActivityTransformedSink = "parquet-weekly-goal-activity-transformed-source"

  val session: SparkSession = SparkSessionUtils.getSession(WeeklyGoalActivityTransformService)
  val service = new SparkBatchService(WeeklyGoalActivityTransformService, session)

  lazy val WeeklyGoalActivityCols = Map(
    "weeklyGoalId" -> "fwga_weekly_goal_id",
    "completedActivity" -> "fwga_completed_activity_id",
    "goalStatus" -> "fwga_weekly_goal_status",
    "eventDateDw" -> "fwga_date_dw_id",
    "occurredOn" -> "occurredOn"
  )

  def main(args: Array[String]): Unit = {
    val transformer = new WeeklyGoalActivityTransform(session, service)
    service.run(transformer.transform())
  }
}
