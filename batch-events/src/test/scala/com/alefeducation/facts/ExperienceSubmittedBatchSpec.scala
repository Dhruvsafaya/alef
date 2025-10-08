package com.alefeducation.facts

import java.util.TimeZone

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.util.DataFrameEqualityUtils.{assertSmallDatasetEquality, createDfFromJson}
import com.alefeducation.util.Helpers._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, SparkSession}

class ExperienceSubmittedBatchSpec extends SparkSuite {

  trait Setup {
    implicit val transformer: ExperienceSubmittedBatch = new ExperienceSubmittedBatch(ExperienceSubmittedName, spark)
  }

  test("transform experience submitted event successfully") {
    val expParquetExperienceSubmittedJson = expS3Json(
      """,
        |"materialType":"INSTRUCTIONAL_PLAN",
        |"materialId":"86b5f93d-0486-4634-9adc-342ff6871ced"
        |""".stripMargin)

    val extraFields =
      """,
        |"materialType":"INSTRUCTIONAL_PLAN",
        |"materialId":"86b5f93d-0486-4634-9adc-342ff6871ced"
        |""".stripMargin

    val msg = experienceSubmittedMsgTemplate(extraFields)

    executeTest(msg, expParquetExperienceSubmittedJson)
  }

  test("transform experience submitted event with material null columns") {
    val materialFields =
      """,
        |"materialType":null,
        |"materialId":null
        |""".stripMargin

    val expParquetExperienceSubmittedJson = expS3Json(materialFields)
    val msg = experienceSubmittedMsgTemplate(materialFields)

    executeTest(msg, expParquetExperienceSubmittedJson)
  }

  test("transform experience submitted event without material columns") {
    val expParquet = expS3Json()

    executeTest(experienceSubmittedMsgTemplate(), expParquet)
  }

  test("transform experience submitted event with replayed=false") {
    val replayedFields =
      """,
        |"replayed":false
        |""".stripMargin

    val expParquetExperienceSubmittedJson = expS3Json(replayedFields)
    val msg = experienceSubmittedMsgTemplate(replayedFields)

    executeTest(msg, expParquetExperienceSubmittedJson)
  }

  def expS3Json(append: String = ""): String = {
    s"""
       |[
       |{"academicYearId":"b518b5cf-89aa-4d27-9668-282e52bd509d","attempt":1,"classId":"fe844274-4942-4149-9086-5a3d66c22d86","contentAcademicYear":"2021","contentId":"8b46bb31-4dff-4a1e-ba25-7a772a1ecc4a","contentPackageId":"8b46bb31-4dff-4a1e-ba25-7a772a1ecc4a","contentPackageItem":{"contentType":"ASGN","contentUuid":"8b46bb31-4dff-4a1e-ba25-7a772a1ecc4a","title":"Assignment"},"contentTitle":"Assignment","contentType":"ASGN","eventRoute":"learning.experience.submitted","eventSemanticId":"b2286219-3250-4b41-8b54-c3180178a4e0","eventType":"ExperienceSubmitted","experienceId":"b2286219-3250-4b41-8b54-c3180178a4e0","instructionalPlanId":"86b5f93d-0486-4634-9adc-342ff6871ced","learningObjectiveId":"56e48f8c-f107-4f17-b0a4-000000000071","learningPathId":"577e7769-ef36-40ac-b5ef-c792b2ce2908","learningSessionId":"12bce8c1-8d13-4ed5-bde1-06e2af45d234","lessonCategory":"INSTRUCTIONAL_LESSON","lessonType":"ASGN","occurredOn":"2020-10-21T09:53:36.164","outsideOfSchool":true,"redo":false,"schoolId":"4a337e99-0d8b-4c5a-a79b-c14ac4196365","startTime":"2020-10-21T09:53:17.592","studentGradeId":"17cebce2-36c3-472f-81d8-808860a9d94d","studentId":"15bde61f-4aba-4f5c-aeaf-7bef7af3aecd","studentK12Grade":6,"studentSection":"3c8d2730-d4a4-4a3c-93fc-00c0f282c66e","subjectCode":"ENGLISH_MOE","subjectId":"f91fe1f6-8d2f-414f-84ca-65ac6079c540","subjectName":"English","suid":"1296a49c-9d11-4555-a484-d208f95a762a","tenantId":"f33276c4-2932-4ee3-bfc6-f317eeb287ca","trimesterOrder":1,"uuid":"3665a691-ef4c-4508-914f-fd76720e9ef5","eventdate":"2020-10-21",
       |    "abbreviation": "TEQ_1",
       |    "activityType": "INSTRUCTIONAL_LESSON",
       |    "exitTicket": true,
       |    "mainComponent": false,
       |    "completionNode": false,
       |    "activityComponentType": "ASSESSMENT",
       |    "teachingPeriodId": null,
       |    "academicYear": "2024" $append
       |}
       |]
      """.stripMargin
  }

  val expRsJson: String = {
    s"""
       |[
       |{"fes_start_time":"2020-10-21 09:53:17.592","grade_uuid":"17cebce2-36c3-472f-81d8-808860a9d94d","exp_uuid":"b2286219-3250-4b41-8b54-c3180178a4e0","lp_uuid":"577e7769-ef36-40ac-b5ef-c792b2ce2908","school_uuid":"4a337e99-0d8b-4c5a-a79b-c14ac4196365","fes_id":"3665a691-ef4c-4508-914f-fd76720e9ef5","fes_lesson_category":"INSTRUCTIONAL_LESSON","fes_is_retry":false,"academic_year_uuid":"b518b5cf-89aa-4d27-9668-282e52bd509d","fes_content_package_id":"8b46bb31-4dff-4a1e-ba25-7a772a1ecc4a","section_uuid":"3c8d2730-d4a4-4a3c-93fc-00c0f282c66e","fes_suid":"1296a49c-9d11-4555-a484-d208f95a762a","fes_instructional_plan_id":"86b5f93d-0486-4634-9adc-342ff6871ced","subject_uuid":"f91fe1f6-8d2f-414f-84ca-65ac6079c540","fes_lesson_type":"ASGN","fes_content_academic_year":"2021","fes_content_type":"ASGN","fes_outside_of_school":true,"fes_ls_id":"12bce8c1-8d13-4ed5-bde1-06e2af45d234","fes_academic_period_order":1,"fes_step_id":"8b46bb31-4dff-4a1e-ba25-7a772a1ecc4a","fes_content_title":"Assignment","fes_attempt":1,"tenant_uuid":"f33276c4-2932-4ee3-bfc6-f317eeb287ca","class_uuid":"fe844274-4942-4149-9086-5a3d66c22d86","lo_uuid":"56e48f8c-f107-4f17-b0a4-000000000071","student_uuid":"15bde61f-4aba-4f5c-aeaf-7bef7af3aecd","eventdate":"2020-10-21","fes_date_dw_id":"20201021","fes_created_time":"2020-10-21T09:53:36.164Z","fes_dw_created_time":"2020-11-30T08:18:07.986Z",
       |    "fes_abbreviation": "TEQ_1",
       |    "fes_activity_type": "INSTRUCTIONAL_LESSON",
       |    "fes_exit_ticket": true,
       |    "fes_main_component": false,
       |    "fes_completion_node": false,
       |    "fes_activity_component_type": "ASSESSMENT",
       |    "fes_material_type":"INSTRUCTIONAL_PLAN",
       |    "fes_material_id":"86b5f93d-0486-4634-9adc-342ff6871ced",
       |    "fes_open_path_enabled": null,
       |    "fes_teaching_period_id": null,
       |    "fes_academic_year": "2024"
       |}
       |]
      """.stripMargin
  }

  val expDeltaJson: String = {
    s"""
       |[
       |{"fes_start_time":"2020-10-21 09:53:17.592","fes_grade_id":"17cebce2-36c3-472f-81d8-808860a9d94d","fes_exp_id":"b2286219-3250-4b41-8b54-c3180178a4e0","fes_lp_id":"577e7769-ef36-40ac-b5ef-c792b2ce2908","fes_school_id":"4a337e99-0d8b-4c5a-a79b-c14ac4196365","fes_id":"3665a691-ef4c-4508-914f-fd76720e9ef5","fes_lesson_category":"INSTRUCTIONAL_LESSON","fes_is_retry":false,"fes_academic_year_id":"b518b5cf-89aa-4d27-9668-282e52bd509d","fes_content_package_id":"8b46bb31-4dff-4a1e-ba25-7a772a1ecc4a","fes_section_id":"3c8d2730-d4a4-4a3c-93fc-00c0f282c66e","fes_suid":"1296a49c-9d11-4555-a484-d208f95a762a","fes_instructional_plan_id":"86b5f93d-0486-4634-9adc-342ff6871ced","fes_subject_id":"f91fe1f6-8d2f-414f-84ca-65ac6079c540","fes_lesson_type":"ASGN","fes_content_academic_year":"2021","fes_content_type":"ASGN","fes_outside_of_school":true,"fes_ls_id":"12bce8c1-8d13-4ed5-bde1-06e2af45d234","fes_academic_period_order":1,"fes_step_id":"8b46bb31-4dff-4a1e-ba25-7a772a1ecc4a","fes_content_title":"Assignment","fes_attempt":1,"fes_tenant_id":"f33276c4-2932-4ee3-bfc6-f317eeb287ca","fes_class_id":"fe844274-4942-4149-9086-5a3d66c22d86","fes_lo_id":"56e48f8c-f107-4f17-b0a4-000000000071","fes_student_id":"15bde61f-4aba-4f5c-aeaf-7bef7af3aecd","eventdate":"2020-10-21","fes_date_dw_id":"20201021","fes_created_time":"2020-10-21T09:53:36.164Z","fes_dw_created_time":"2020-11-30T08:18:07.986Z",
       |    "fes_abbreviation": "TEQ_1",
       |    "fes_activity_type": "INSTRUCTIONAL_LESSON",
       |    "fes_exit_ticket": true,
       |    "fes_main_component": false,
       |    "fes_completion_node": false,
       |    "fes_activity_component_type": "ASSESSMENT",
       |    "fes_material_type":"INSTRUCTIONAL_PLAN",
       |    "fes_material_id":"86b5f93d-0486-4634-9adc-342ff6871ced",
       |    "fes_open_path_enabled": null,
       |    "fes_teaching_period_id": null,
       |    "fes_academic_year": "2024"
       |}
       |]
      """.stripMargin
  }

  def experienceSubmittedMsgTemplate(append: String = ""): String = {
    s"""
      |{
      |  "tenantId": "f33276c4-2932-4ee3-bfc6-f317eeb287ca",
      |  "eventType": "ExperienceSubmitted",
      |  "uuid": "3665a691-ef4c-4508-914f-fd76720e9ef5",
      |  "occurredOn": "2020-10-21T09:53:36.164",
      |  "attempt": 1,
      |  "experienceId": "b2286219-3250-4b41-8b54-c3180178a4e0",
      |  "learningSessionId": "12bce8c1-8d13-4ed5-bde1-06e2af45d234",
      |  "learningObjectiveId": "56e48f8c-f107-4f17-b0a4-000000000071",
      |  "studentId": "15bde61f-4aba-4f5c-aeaf-7bef7af3aecd",
      |  "schoolId": "4a337e99-0d8b-4c5a-a79b-c14ac4196365",
      |  "studentK12Grade": 6,
      |  "studentGradeId": "17cebce2-36c3-472f-81d8-808860a9d94d",
      |  "studentSection": "3c8d2730-d4a4-4a3c-93fc-00c0f282c66e",
      |  "learningPathId": "577e7769-ef36-40ac-b5ef-c792b2ce2908",
      |  "instructionalPlanId": "86b5f93d-0486-4634-9adc-342ff6871ced",
      |  "subjectId": "f91fe1f6-8d2f-414f-84ca-65ac6079c540",
      |  "subjectCode": "ENGLISH_MOE",
      |  "contentPackageId": "8b46bb31-4dff-4a1e-ba25-7a772a1ecc4a",
      |  "contentId": "8b46bb31-4dff-4a1e-ba25-7a772a1ecc4a",
      |  "contentTitle": "Assignment",
      |  "lessonType": "ASGN",
      |  "contentPackageItem": {
      |    "contentUuid": "8b46bb31-4dff-4a1e-ba25-7a772a1ecc4a",
      |    "title": "Assignment",
      |    "contentType": "ASGN"
      |  },
      |  "subjectName": "English",
      |  "academicYearId": "b518b5cf-89aa-4d27-9668-282e52bd509d",
      |  "contentAcademicYear": "2021",
      |  "classId": "fe844274-4942-4149-9086-5a3d66c22d86",
      |  "outsideOfSchool": true,
      |  "contentType": "ASGN",
      |  "lessonCategory": "INSTRUCTIONAL_LESSON",
      |  "suid": "1296a49c-9d11-4555-a484-d208f95a762a",
      |  "startTime": "2020-10-21T09:53:17.592",
      |  "trimesterOrder": 1,
      |  "eventRoute": "learning.experience.submitted",
      |  "eventSemanticId": "b2286219-3250-4b41-8b54-c3180178a4e0",
      |  "redo": false,
      |  "abbreviation": "TEQ_1",
      |  "activityType": "INSTRUCTIONAL_LESSON",
      |  "exitTicket": true,
      |  "mainComponent": false,
      |  "completionNode": false,
      |  "activityComponentType": "ASSESSMENT",
      |  "teachingPeriodId": null,
      |  "academicYear": "2024" $append
      |}""".stripMargin
  }

  def executeTest(msg: String, expS3Json: String, expRsJson: String = expRsJson, expDeltaJson: String = expDeltaJson): Unit = {
    val expRedshiftExperienceSubmittedDf = createDfFromJsonWithTimeCols(spark, expRsJson)
    val expDeltaExperienceSubmittedDf = createDfFromJsonWithTimeCols(spark, expDeltaJson)

    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetExperienceSubmittedSource,
          value = msg
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>

          val parquetExperienceSubmittedDf = sinks.find(_.name === ParquetExperienceSubmittedSink).get.output
          val redshiftExperienceSubmittedDf = sinks.find(_.name === RedshiftExperienceSubmittedSink)
            .get.output
          val deltaExperienceSubmittedDf = sinks.find(_.name === DeltaExperienceSubmittedSink)
            .get.output

          assertSmallDatasetEquality(spark, FactExperienceSubmittedEntity, parquetExperienceSubmittedDf, expS3Json)
          assertSmallDatasetEquality(FactExperienceSubmittedEntity, redshiftExperienceSubmittedDf, expRedshiftExperienceSubmittedDf)
          assertSmallDatasetEquality(FactExperienceSubmittedEntity, deltaExperienceSubmittedDf, expDeltaExperienceSubmittedDf)
        }
      )
    }
  }

  def createDfFromJsonWithTimeCols(spark: SparkSession, json: String): DataFrame = {
    createDfFromJson(spark, json)
      .withColumn("fes_created_time", col("fes_created_time").cast(TimestampType))
      .withColumn("fes_start_time", col("fes_start_time").cast(TimestampType))
  }
}
