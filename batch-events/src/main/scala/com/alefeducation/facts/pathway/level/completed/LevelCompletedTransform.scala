package com.alefeducation.facts.pathway.level.completed

import com.alefeducation.base.SparkBatchService
import com.alefeducation.service.DataSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import org.apache.spark.sql.types.TimestampType

import scala.collection.immutable.Map

class LevelCompletedTransform(val session: SparkSession,
                                  val service: SparkBatchService) {

  import LevelCompletedTransform._
  import session.implicits._

  def transform(): Option[DataSink] = {
    val levelCompletedParquet = service.readOptional(ParquetLevelCompletedSource, session, extraProps = List(("mergeSchema", "true")))

    val levelCompletedEvents = levelCompletedParquet.map(
      _.withColumn("completedOn", $"completedOn".cast(TimestampType))
        .addColIfNotExists("academicYear", schemaFor[String].dataType)
        .addColIfNotExists("score", schemaFor[Int].dataType))

      val levelCompletedTransformed = levelCompletedEvents.map(
        _.transformForInsertFact(LevelCompletedCols, FactLevelCompletedEntity))

    levelCompletedTransformed.map(df => {
      DataSink(ParquetLevelCompletedTransformedSink, df)
    })
  }

}

object LevelCompletedTransform {

  val LevelCompletedTransformService = "transform-level-completed"
  val ParquetLevelCompletedSource = "parquet-level-completed-source"
  val ParquetLevelCompletedTransformedSink = "level-completed-transformed-sink"
  val FactLevelCompletedEntity = "flc"

  val session: SparkSession = SparkSessionUtils.getSession(LevelCompletedTransformService)
  val service = new SparkBatchService(LevelCompletedTransformService, session)

  lazy val LevelCompletedCols = Map(
      "completedOn" -> "flc_completed_on",
      "learnerId" -> "flc_student_id",
      "tenantId" -> "flc_tenant_id",
      "classId" -> "flc_class_id",
      "pathwayId" -> "flc_pathway_id",
      "levelId" -> "flc_level_id",
      "totalStars" -> "flc_total_stars",
      "occurredOn" -> "occurredOn",
      "academicYear" -> "flc_academic_year",
      "score" -> "flc_score"
  )

  def main(args: Array[String]): Unit = {
    val transformer = new LevelCompletedTransform(session, service)
    service.run(transformer.transform())
  }
}
