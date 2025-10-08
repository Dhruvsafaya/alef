package com.alefeducation.facts.badge_awarded

import com.alefeducation.base.SparkBatchService
import com.alefeducation.service.DataSink
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.LongType

class BadgeAwardedTransform(val session: SparkSession, val service: SparkBatchService) {

  import BadgeAwardedTransform._
  import com.alefeducation.util.BatchTransformerUtility._

  def transform(): Option[DataSink] = {
    val source = service.readOptional(BadgeAwardedSource, session)

    val transformed = source.map(
      _.transformForInsertFact(BadgeAwardedCols, BadgeAwardedEntity)
        .withColumn(s"${BadgeAwardedEntity}_date_dw_id", col(s"${BadgeAwardedEntity}_date_dw_id").cast(LongType))
    )

    transformed.map(DataSink(BadgeAwardedSink, _))
  }
}

object BadgeAwardedTransform {

  val BadgeAwardedTransformService = "transform-badge-awarded"
  val BadgeAwardedSource = "parquet-badge-awarded-source"
  val BadgeAwardedSink = "badge-awarded-transformed-sink"
  val BadgeAwardedEntity = "fba"

  val session = SparkSessionUtils.getSession(BadgeAwardedTransformService)
  val service = new SparkBatchService(BadgeAwardedTransformService, session)

  val BadgeAwardedCols: Map[String, String] = Map(
    "tenantId" -> s"${BadgeAwardedEntity}_tenant_id",
    "id" -> s"${BadgeAwardedEntity}_id",
    "badgeTypeId" -> s"${BadgeAwardedEntity}_badge_type_id",
    "badgeType" -> s"${BadgeAwardedEntity}_badge_type",
    "tier" -> s"${BadgeAwardedEntity}_tier",
    "studentId" -> s"${BadgeAwardedEntity}_student_id",
    "sectionId" -> s"${BadgeAwardedEntity}_section_id",
    "gradeId" -> s"${BadgeAwardedEntity}_grade_id",
    "academicYearId" -> s"${BadgeAwardedEntity}_academic_year_id",
    "schoolId" -> s"${BadgeAwardedEntity}_school_id",
    "occurredOn" -> "occurredOn"
  )

  def main(args: Array[String]): Unit = {
    val transform = new BadgeAwardedTransform(session, service)
    service.run(transform.transform())
  }
}
