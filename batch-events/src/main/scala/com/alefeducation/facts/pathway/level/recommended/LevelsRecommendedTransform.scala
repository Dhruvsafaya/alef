package com.alefeducation.facts.pathway.level.recommended

import com.alefeducation.base.SparkBatchService
import com.alefeducation.facts.pathway.activity.PathwayActivityCompletedTransform.{ACTIVITY, INTERIM_CHECKPOINT}
import com.alefeducation.service.DataSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants.{GuardianFeedbackReadEvent, GuardianMessageSentEvent, TeacherFeedbackDeletedEvent, TeacherFeedbackSentEvent, TeacherMessageDeletedEvent, TeacherMessageSentEvent}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import org.apache.spark.sql.functions.{col, explode, lit, typedLit, when}
import org.apache.spark.sql.types.TimestampType

import scala.collection.immutable.Map

class LevelsRecommendedTransform(val session: SparkSession,
                                 val service: SparkBatchService) {

  import LevelsRecommendedTransform._
  import session.implicits._

  def transform(): Option[DataSink] = {
    val levelsRecommendedParquet = service.readOptional(ParquetLevelsRecommendedSource, session, extraProps = List(("mergeSchema", "true")))

    val levelsRecommendedTransformed = levelsRecommendedParquet.map(
      _.select(
        col("*"),
        explode(col("recommendedLevels")).as("rlevels")
      ).withColumn("recommendedOn", $"recommendedOn".cast(TimestampType))
        .withColumn("status",
          when($"rlevels.status" === "ACTIVE", lit(ACTIVE))
            .otherwise(UPCOMING)
        ).withColumn("recommendationType", eventRecommendationTypeMap(col("recommendationType")))
        .addColIfNotExists("academicYear", schemaFor[String].dataType)
        .transformForInsertFact(LevelsRecommendedCols, FactLevelsRecommendedEntity)
    )


    levelsRecommendedTransformed.map(df => {
      DataSink(ParquetLevelsRecommendedTransformedSink, df)
    })
  }

}

object LevelsRecommendedTransform {

  val LevelsRecommendedTransformService = "transform-levels-recommended"
  val ParquetLevelsRecommendedSource = "parquet-levels-recommended-source"
  val ParquetLevelsRecommendedTransformedSink = "levels-recommended-transformed-sink"
  val FactLevelsRecommendedEntity = "flr"

  val ACTIVE = 1
  val UPCOMING = 2


   val eventRecommendationTypeMap = typedLit(
    Map(
      "PLACEMENT_COMPLETION" -> 1,
      "LEVEL_COMPLETION" -> 2,
      "GRADE_CHANGE" -> 3,
      "MANUAL_PLACEMENT" -> 4,
      "BY_GRADE" -> 5,
      "AT_COURSE_BEGINNING" -> 6
    ))


  val session: SparkSession = SparkSessionUtils.getSession(LevelsRecommendedTransformService)
  val service = new SparkBatchService(LevelsRecommendedTransformService, session)

  lazy val LevelsRecommendedCols = Map(
    "recommendedOn" -> "flr_recommended_on",
    "learnerId" -> "flr_student_id",
    "tenantId" -> "flr_tenant_id",
    "classId" -> "flr_class_id",
    "pathwayId" -> "flr_pathway_id",
    "completedLevelId" -> "flr_completed_level_id",
    "rLevels.id" -> "flr_level_id",
    "status" -> "flr_status",
    "recommendationType" -> "flr_recommendation_type",
    "occurredOn" -> "occurredOn",
    "academicYear" -> "flr_academic_year"
  )

  def main(args: Array[String]): Unit = {
    val transformer = new LevelsRecommendedTransform(session, service)
    service.run(transformer.transform())
  }
}
