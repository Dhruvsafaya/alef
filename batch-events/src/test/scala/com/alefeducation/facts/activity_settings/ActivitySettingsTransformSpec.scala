package com.alefeducation.facts.activity_settings

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.facts.activity_settings.ActivitySettingsTransform.{ActivitySettingsEntity, ActivitySettingsKey, ActivitySettingsTransformService}
import com.alefeducation.util.DataFrameEqualityUtils.{assertSmallDatasetEquality, createDfFromJson}
import com.alefeducation.util.Resources.{getNestedString, getSink}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mockito.Mockito.when
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatestplus.mockito.MockitoSugar.mock

class ActivitySettingsTransformSpec extends SparkSuite {

  val service: SparkBatchService = mock[SparkBatchService]

  val expectedColumns: Set[String] = Set(
    "eventdate",
    "fas_activity_id",
    "fas_class_gen_subject_name",
    "fas_class_id",
    "fas_created_time",
    "fas_date_dw_id",
    "fas_dw_created_time",
    "fas_dw_id",
    "fas_grade_id",
    "fas_k12_grade",
    "fas_open_path_enabled",
    "fas_school_id",
    "fas_teacher_id",
    "fas_tenant_id"
  )

  test("should create activity settings dataframe") {

    val expJson =
      """
          [
        |    {
        |        "eventdate": "2024-05-21",
        |        "fas_activity_id": "5ed7df8f-be87-481d-b183-8e3a1f745911",
        |        "fas_class_id": "5ed7df8f-be87-481d-b183-8e3a1f745911",
        |        "fas_created_time": "2024-05-21T00:00:00.000Z",
        |        "fas_date_dw_id": "20240521",
        |        "fas_dw_created_time": "2024-05-21T07:43:08.926Z",
        |        "fas_dw_id": 1,
        |        "fas_grade_id": "5ed7df8f-be87-481d-b183-8e3a1f745911",
        |        "fas_k12_grade": 7,
        |        "fas_open_path_enabled": true,
        |        "fas_school_id": "5ed7df8f-be87-481d-b183-8e3a1f745911",
        |        "fas_class_gen_subject_name": "English",
        |        "fas_teacher_id": "5ed7df8f-be87-481d-b183-8e3a1f745911",
        |        "fas_tenant_id": "5ed7df8f-be87-481d-b183-8e3a1f745911"
        |    },
        |    {
        |        "eventdate": "2024-05-21",
        |        "fas_activity_id": "5ed7df8f-be87-481d-b183-8e3a1f745911",
        |        "fas_class_id": "5ed7df8f-be87-481d-b183-8e3a1f745911",
        |        "fas_created_time": "2024-05-21T00:00:00.000Z",
        |        "fas_date_dw_id": "20240521",
        |        "fas_dw_created_time": "2024-05-21T07:43:08.926Z",
        |        "fas_dw_id": 2,
        |        "fas_grade_id": "5ed7df8f-be87-481d-b183-8e3a1f745911",
        |        "fas_k12_grade": 7,
        |        "fas_open_path_enabled": false,
        |        "fas_school_id": "5ed7df8f-be87-481d-b183-8e3a1f745911",
        |        "fas_class_gen_subject_name": "English",
        |        "fas_teacher_id": "5ed7df8f-be87-481d-b183-8e3a1f745911",
        |        "fas_tenant_id": "5ed7df8f-be87-481d-b183-8e3a1f745911"
        |    }
        |]
          |""".stripMargin
    val expDf = createDfFromJsonWithTimeCols(spark, expJson)

    val openPathEnabledEvent =
      """
          |[
          |{
          |  "eventType": "ActivitySettingsOpenPathEnabledEvent",
          |  "tenantId": "5ed7df8f-be87-481d-b183-8e3a1f745911",
          |  "classId": "5ed7df8f-be87-481d-b183-8e3a1f745911",
          |  "activityId": "5ed7df8f-be87-481d-b183-8e3a1f745911",
          |  "teacherId": "5ed7df8f-be87-481d-b183-8e3a1f745911",
          |  "schoolId": "5ed7df8f-be87-481d-b183-8e3a1f745911",
          |  "subjectName": "English",
          |  "gradeId": "5ed7df8f-be87-481d-b183-8e3a1f745911",
          |  "gradeLevel": 7,
          |  "openPathEnabled": true,
          |  "occurredOn": "2024-05-21T00:00:00.000"
          |}
          |]
          """.stripMargin

    val openPathDisabledEvent =
      """
          |[
          |{
          |  "eventType": "ActivitySettingsOpenPathDisabledEvent",
          |  "tenantId": "5ed7df8f-be87-481d-b183-8e3a1f745911",
          |  "classId": "5ed7df8f-be87-481d-b183-8e3a1f745911",
          |  "activityId": "5ed7df8f-be87-481d-b183-8e3a1f745911",
          |  "teacherId": "5ed7df8f-be87-481d-b183-8e3a1f745911",
          |  "schoolId": "5ed7df8f-be87-481d-b183-8e3a1f745911",
          |  "subjectName": "English",
          |  "gradeId": "5ed7df8f-be87-481d-b183-8e3a1f745911",
          |  "gradeLevel": 7,
          |  "openPathEnabled": false,
          |  "occurredOn": "2024-05-21T00:00:00.000"
          |}
          |]
          """.stripMargin

    val sprk = spark
    import sprk.implicits._
    val transformer = new ActivitySettingsTransform(sprk, service)
    val enabledInputDF = spark.read.json(Seq(openPathEnabledEvent).toDS())
    val disabledInputDF = spark.read.json(Seq(openPathDisabledEvent).toDS())
    val combinedDF = enabledInputDF.unionAll(disabledInputDF)

    val sourceName = getNestedString(ActivitySettingsTransformService, "activity-settings-open-path-source")

    val sinkName = getSink(ActivitySettingsTransformService).head
    when(service.readOptional(sourceName, sprk)).thenReturn(Some(combinedDF))
    when(service.getStartIdUpdateStatus(ActivitySettingsKey)).thenReturn(1)

    val df = transformer.transform(sourceName, sinkName).get.output

    assert(df.columns.toSet === expectedColumns)
    assertSmallDatasetEquality(ActivitySettingsEntity, df, expDf)
    assert(df.schema("fas_k12_grade").dataType === IntegerType)
  }

  private def createDfFromJsonWithTimeCols(spark: SparkSession, json: String): DataFrame = {
    createDfFromJson(spark, json)
      .withColumn("fas_created_time", col("fas_created_time").cast(TimestampType))
  }
}
