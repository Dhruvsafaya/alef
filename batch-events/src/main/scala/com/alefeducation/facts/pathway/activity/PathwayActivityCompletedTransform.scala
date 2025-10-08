package com.alefeducation.facts.pathway.activity

import com.alefeducation.base.SparkBatchService
import com.alefeducation.service.DataSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import org.apache.spark.sql.functions.{lit, when}

import scala.collection.immutable.Map

class PathwayActivityCompletedTransform(val session: SparkSession,
                                        val service: SparkBatchService) {

  import PathwayActivityCompletedTransform._
  import session.implicits._

  def transform(): Option[DataSink] = {
    val pathwayActivityCompletedParquet = service.readOptional(ParquetPathwayActivityCompletedSource, session, extraProps = List(("mergeSchema", "true")))
    val pathwayActivityCompletedSource = pathwayActivityCompletedParquet.map(
      _.addColIfNotExists("timeSpent", schemaFor[Int].dataType)
        .addColIfNotExists("attempt", schemaFor[Int].dataType)
        .addColIfNotExists("learningSessionId", schemaFor[String].dataType)
        .addColIfNotExists("academicYear", schemaFor[String].dataType)
     )

    val pathwayActivityCompletedTransformed = pathwayActivityCompletedSource.map(
      _.withColumn("activityType",
        when($"activityType" === "ACTIVITY", lit(ACTIVITY))
          .otherwise(INTERIM_CHECKPOINT))
        transformForInsertFact(PathwayActivityCompletedCols, FactPathwayActivityCompletedEntity))

    pathwayActivityCompletedTransformed.map(df => {
      DataSink(ParquetPathwayActivityCompletedTransformedSink, df)
    })
  }

}

object PathwayActivityCompletedTransform {

  val PathwayActivityCompletedTransformService = "transform-pathway-activity-completed"
  val ParquetPathwayActivityCompletedSource = "parquet-pathway-activity-completed-source"
  val ParquetPathwayActivityCompletedTransformedSink = "pathway-activity-completed-transformed-sink"
  val FactPathwayActivityCompletedEntity = "fpac"

  val session: SparkSession = SparkSessionUtils.getSession(PathwayActivityCompletedTransformService)
  val service = new SparkBatchService(PathwayActivityCompletedTransformService, session)

  val ACTIVITY = 1
  val INTERIM_CHECKPOINT = 2

  lazy val PathwayActivityCompletedCols = Map(
    "tenantId" -> "fpac_tenant_id",
    "learnerId" -> "fpac_student_id",
    "classId" -> "fpac_class_id",
    "pathwayId" -> "fpac_pathway_id",
    "levelId" -> "fpac_level_id",
    "activityId" -> "fpac_activity_id",
    "activityType" -> "fpac_activity_type",
    "score" -> "fpac_score",
    "occurredOn" -> "occurredOn",
    "timeSpent" -> "fpac_time_spent",
    "learningSessionId" -> "fpac_learning_session_id",
    "attempt" -> "fpac_attempt",
    "academicYear" -> "fpac_academic_year"
  )

  def main(args: Array[String]): Unit = {
    val transformer = new PathwayActivityCompletedTransform(session, service)
    service.run(transformer.transform())
  }
}
