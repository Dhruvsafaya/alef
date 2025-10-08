package com.alefeducation.facts.activity_settings

import com.alefeducation.base.SparkBatchService
import com.alefeducation.facts.activity_settings.ActivitySettingsTransform.{
  ActivitySettingsCols,
  ActivitySettingsEntity,
  ActivitySettingsKey
}
import com.alefeducation.facts.announcement.AnnouncementUtils.ActiveStatus
import com.alefeducation.schema.internal.ControlTableUtils.ProductMaxIdType
import com.alefeducation.service.DataSink
import com.alefeducation.util.Resources.{getNestedString, getSink}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.{IntegerType, TimestampType}

class ActivitySettingsTransform(val session: SparkSession, val service: SparkBatchService) {

  import com.alefeducation.util.BatchTransformerUtility._

  def transform(source: String, sink: String): Option[DataSink] = {
    val sourceDF = service.readOptional(source, session)

    val startId = service.getStartIdUpdateStatus(ActivitySettingsKey)

    sourceDF
      .map(
        _.transformForInsertFact(ActivitySettingsCols, ActivitySettingsEntity)
          .genDwId(s"${ActivitySettingsEntity}_dw_id", startId, s"${ActivitySettingsEntity}_created_time")
          .withColumn(s"${ActivitySettingsEntity}_k12_grade", col(s"${ActivitySettingsEntity}_k12_grade").cast(IntegerType))
      )
      .map(DataSink(sinkName = sink, _, controlTableUpdateOptions = Map(ProductMaxIdType -> ActivitySettingsKey)))
  }
}

object ActivitySettingsTransform {
  val ActivitySettingsTransformService = "activity-settings-open-path-transform"
  val ActivitySettingsEntity = "fas"

  val ActivitySettingsKey = "fact_activity_setting"

  val ActivitySettingsCols: Map[String, String] = Map(
    "tenantId" -> s"${ActivitySettingsEntity}_tenant_id",
    "classId" -> s"${ActivitySettingsEntity}_class_id",
    "activityId" -> s"${ActivitySettingsEntity}_activity_id",
    "teacherId" -> s"${ActivitySettingsEntity}_teacher_id",
    "schoolId" -> s"${ActivitySettingsEntity}_school_id",
    "subjectName" -> s"${ActivitySettingsEntity}_class_gen_subject_name",
    "gradeId" -> s"${ActivitySettingsEntity}_grade_id",
    "gradeLevel" -> s"${ActivitySettingsEntity}_k12_grade",
    "openPathEnabled" -> s"${ActivitySettingsEntity}_open_path_enabled",
    "occurredOn" -> "occurredOn"
  )

  val session: SparkSession = SparkSessionUtils.getSession(ActivitySettingsTransformService)
  val service = new SparkBatchService(ActivitySettingsTransformService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new ActivitySettingsTransform(session, service)
    val sourceName = getNestedString(ActivitySettingsTransformService, "activity-settings-open-path-source")
    val sinkName = getSink(ActivitySettingsTransformService).head
    service.run(transformer.transform(sourceName, sinkName))
  }
}
