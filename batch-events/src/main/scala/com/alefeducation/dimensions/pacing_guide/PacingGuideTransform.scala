package com.alefeducation.dimensions.pacing_guide

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.pacing_guide.PacingGuideTransform.{PacingGuideColMapping, PacingGuideEntityPrefix, PacingGuideTransformedSink, pacingGuideAddedParquetSource, pacingGuideDeletedParquetSource, pacingGuideUpdatedParquetSource}
import com.alefeducation.schema.internal.ControlTableUtils.ProductMaxIdType
import com.alefeducation.service.DataSink
import com.alefeducation.util.Helpers.Deleted
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.alefeducation.util.BatchTransformerUtility._
import org.apache.spark.sql.Row.empty.schema
import org.apache.spark.sql.functions.{col, explode, lit, size, when}
import org.apache.spark.sql.types.{DateType, TimestampType}

class PacingGuideTransform(val session: SparkSession, val service: SparkBatchService) {

  def readCommonPacingGuide(parquetSource: String,
                            session: SparkSession,
                            service: SparkBatchService): Option[DataFrame] = {
    val transformedEvent = service.readOptional(parquetSource, session).map(
      _.withColumn("occurredOn", col("occurredOn").cast(TimestampType))
        .withColumn("exploded_activity", explode(col("activities")))
        .withColumn("pacing_activity_id", col("exploded_activity.activity.id"))
        .withColumn("pacing_activity_order", col("exploded_activity.activity.order"))
        .withColumn("pacing_period_id", when(col("academicCalendarId").isNotNull, col("exploded_activity.associations").getItem(0).getItem("id")).otherwise(lit(null)))
        .withColumn("pacing_period_label", when(col("academicCalendarId").isNotNull, col("exploded_activity.associations").getItem(0).getItem("label")).otherwise(lit(null)))
        .withColumn("pacing_period_start_date", when(col("academicCalendarId").isNotNull, col("exploded_activity.associations").getItem(0).getItem("startDate").cast(DateType)).otherwise(lit(null).cast(DateType)))
        .withColumn("pacing_period_end_date", when(col("academicCalendarId").isNotNull, col("exploded_activity.associations").getItem(0).getItem("endDate").cast(DateType)).otherwise(lit(null).cast(DateType)))
        .withColumn("pacing_interval_id", when(col("academicCalendarId").isNotNull, col("exploded_activity.associations").getItem(1).getItem("id")).otherwise(col("exploded_activity.associations").getItem(0).getItem("id")))
        .withColumn("pacing_interval_label", when(col("academicCalendarId").isNotNull, col("exploded_activity.associations").getItem(1).getItem("label")).otherwise(col("exploded_activity.associations").getItem(0).getItem("label")))
        .withColumn("pacing_interval_start_date", when(col("academicCalendarId").isNotNull, col("exploded_activity.associations").getItem(1).getItem("startDate").cast(DateType)).otherwise(col("exploded_activity.associations").getItem(0).getItem("startDate").cast(DateType)))
        .withColumn("pacing_interval_end_date", when(col("academicCalendarId").isNotNull, col("exploded_activity.associations").getItem(1).getItem("endDate").cast(DateType)).otherwise(col("exploded_activity.associations").getItem(0).getItem("endDate").cast(DateType)))
        .withColumn("pacing_interval_type", when(col("academicCalendarId").isNotNull, col("exploded_activity.associations").getItem(1).getItem("type")).otherwise(col("exploded_activity.associations").getItem(0).getItem("type")))
    )
    transformedEvent
  }

  def transform(): List[Option[Sink]] = {
    val pacingGuideAdded: Option[DataFrame] = readCommonPacingGuide(pacingGuideAddedParquetSource, session, service)
    val pacingGuideUpdated: Option[DataFrame] = readCommonPacingGuide(pacingGuideUpdatedParquetSource, session, service)
    val pacingGuideDeleted: Option[DataFrame] = readCommonPacingGuide(pacingGuideDeletedParquetSource, session, service)

    val mutated = pacingGuideAdded
      .unionOptionalByNameWithEmptyCheck(pacingGuideUpdated)
      .unionOptionalByNameWithEmptyCheck(pacingGuideDeleted)

    val startId = service.getStartIdUpdateStatus("dim_pacing_guide")


    val mutatedIWH: Option[DataFrame] = mutated.flatMap(
      _.transformForIWH2(
        PacingGuideColMapping,
        PacingGuideEntityPrefix,
        associationType = 0, //unnecessary field
        attachedEvents = List("PacingGuideCreatedEvent", "PacingGuideUpdatedEvent"),
        detachedEvents = List("PacingGuideDeletedEvent"),
        groupKey = List("id"),
        associationDetachedStatusVal = Deleted,
        inactiveStatus = Deleted
      ).genDwId(s"${PacingGuideEntityPrefix}_dw_id", startId)
        .checkEmptyDf
    )

    List(
      mutatedIWH.map(DataSink(PacingGuideTransformedSink, _, controlTableUpdateOptions = Map(ProductMaxIdType -> "dim_pacing_guide")))
    )
  }
}

object PacingGuideTransform {
  val PacingGuideTransformService = "pacing-guide-mutated-transform"

  val pacingGuideAddedParquetSource = "parquet-pacing-guide-created-source"
  val pacingGuideUpdatedParquetSource = "parquet-pacing-guide-updated-source"
  val pacingGuideDeletedParquetSource = "parquet-pacing-guide-deleted-source"

  val PacingGuideTransformedSink = "parquet-pacing-guide-mutated-transformed-source"

  val PacingGuideEntityPrefix = "pacing"


  val PacingGuideColMapping: Map[String, String] = Map[String, String](
    "id" -> "pacing_id",
    "classId" -> "pacing_class_id",
    "courseId" -> "pacing_course_id",
    "instructionalPlanId" -> "pacing_ip_id",
    "academicCalendarId" -> "pacing_academic_calendar_id",
    "academicYearId" -> "pacing_academic_year_id",
    "occurredOn" -> "occurredOn",
    "pacing_status" -> "pacing_status",
    "pacing_activity_id" -> "pacing_activity_id",
    "pacing_activity_order" -> "pacing_activity_order",
    "pacing_period_id" -> "pacing_period_id",
    "pacing_period_label" -> "pacing_period_label",
    "pacing_period_start_date" -> "pacing_period_start_date",
    "pacing_period_end_date" -> "pacing_period_end_date",
    "pacing_interval_id" -> "pacing_interval_id",
    "pacing_interval_label" -> "pacing_interval_label",
    "pacing_interval_start_date" -> "pacing_interval_start_date",
    "pacing_interval_end_date" -> "pacing_interval_end_date",
    "pacing_interval_type" -> "pacing_interval_type",
    "tenantId" -> "pacing_tenant_id"
  )

  val session: SparkSession = SparkSessionUtils.getSession(PacingGuideTransformService)
  val service = new SparkBatchService(PacingGuideTransformService, session)

  def main(args: Array[String]): Unit = {
    service.runAll {
      val transformer = new PacingGuideTransform(session, service)
      transformer.transform().flatten
    }
  }
}
