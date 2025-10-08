package com.alefeducation.dimensions.course.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class CourseGradeMutatedTransformTest extends SparkSuite {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  test("should construct course grade dataframe from published source") {
    val value =
      """
        |[
        |{
        | "eventType": "CoursePublishedEvent",
        | "courseType": "PATHWAY",
        |	"id": "88fc9cea-85af-45dd-a8a7-43ef7bf5f598",
        |	"organisation": "shared",
        |	"name": "Test Events - 2",
        |	"code": "Test Events - 1",
        |	"subOrganisations": [
        |		"25e9b735-6b6c-403b-a9f3-95e478e8f1ed",
        |		"53055d6d-ecf6-4596-88db-0c50cac72cd0",
        |		"50445c7f-0f0d-45d4-aee1-26919a9b50f5"
        |	],
        |	"subjectId": 754325,
        |	"description": null,
        |	"goal": null,
        |	"modules": [
        |		{
        |			"id": "4478a0ef-d3c4-4fcc-a9eb-415c8ebc9cd1",
        |			"maxAttempts": 1,
        |			"activityId": {
        |				"uuid": "3d97d176-1cc1-4a54-b8ba-000000028407",
        |				"id": 28407
        |			},
        |			"settings": {
        |				"pacing": "LOCKED",
        |				"hideWhenPublishing": false,
        |				"isOptional": false
        |			}
        |		}
        |	],
        |	"occurredOn": "2021-06-23 05:33:24.921",
        | "langCode": "EN_US",
        |	"curriculums": [
        |		{"curriculumId": 1, "gradeId":1, "subjectId": 1},
        |		{"curriculumId": 2, "gradeId":2, "subjectId": 2}
        |	],
        | "gradeIds" : [1,2,3]
        |}
        |]
        """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val inputDF = spark.read.json(Seq(value).toDS())
    val emptyDF = spark.emptyDataFrame

    when(service.readOptional("parquet-course-published-source", sprk, extraProps = List(("mergeSchema", "true"))))
      .thenReturn(Some(inputDF))
    when(service.readOptional("parquet-course-updated-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(emptyDF))

    val transformer = new CourseFieldMutatedTransform(sprk, service, "course-grade-mutated-transform")
    val sinks = transformer.transform()

    val df = sinks.filter(_.name == "parquet-course-grade-transformed-sink").head.output

    assert(df.count() == 3)
    val expectedColumns = Set(
      "cg_dw_id",
      "cg_course_id",
      "cg_grade_id",
      "cg_status",
      "cg_created_time",
      "cg_dw_created_time",
      "cg_updated_time",
      "cg_dw_updated_time"
    )
    assert(df.columns.toSet === expectedColumns)
    assert[String](df, "cg_course_id", "88fc9cea-85af-45dd-a8a7-43ef7bf5f598")
    assert[Int](df, "cg_grade_id", 1)
    assert[Int](df, "cg_status", 1)
    assert[String](df, "cg_created_time", "2021-06-23 05:33:24.921")
  }

  test("should construct course grade dataframe from updated source") {
    val value =
      """
        |[
        |{
        | "eventType": "CourseSettingsUpdatedEvent",
        | "courseType": "PATHWAY",
        |	"id": "88fc9cea-85af-45dd-a8a7-43ef7bf5f598",
        |	"organisation": "shared",
        |	"name": "Test Events - 2",
        |	"code": "Test Events - 1",
        |	"subOrganisations": [
        |		"25e9b735-6b6c-403b-a9f3-95e478e8f1ed",
        |		"53055d6d-ecf6-4596-88db-0c50cac72cd0",
        |		"50445c7f-0f0d-45d4-aee1-26919a9b50f5"
        |	],
        |	"subjectId": 754325,
        |	"description": null,
        |	"goal": null,
        | "courseStatus": "PUBLISHED",
        |	"modules": [
        |		{
        |			"id": "4478a0ef-d3c4-4fcc-a9eb-415c8ebc9cd1",
        |			"maxAttempts": 1,
        |			"activityId": {
        |				"uuid": "3d97d176-1cc1-4a54-b8ba-000000028407",
        |				"id": 28407
        |			},
        |			"settings": {
        |				"pacing": "LOCKED",
        |				"hideWhenPublishing": false,
        |				"isOptional": false
        |			}
        |		}
        |	],
        |	"occurredOn": "2021-06-23 05:33:24.921",
        | "langCode": "EN_US",
        |	"curriculums": [
        |		{"curriculumId": 1, "gradeId":1, "subjectId": 1},
        |		{"curriculumId": 2, "gradeId":2, "subjectId": 2}
        |	],
        | "gradeIds" : [1,2,3],
        | "subjectIds": [4,5,6]
        |}
        |]
        """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val inputDF = spark.read.json(Seq(value).toDS())
    val emptyDF = spark.emptyDataFrame

    when(service.readOptional("parquet-course-published-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(emptyDF))
    when(service.readOptional("parquet-course-updated-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(inputDF))

    val transformer = new CourseFieldMutatedTransform(sprk, service, "course-grade-mutated-transform")
    val sinks = transformer.transform()

    val df = sinks.filter(_.name == "parquet-course-grade-transformed-sink").head.output

    assert(df.count() == 3)
    val expectedColumns = Set(
      "cg_dw_id",
      "cg_course_id",
      "cg_grade_id",
      "cg_status",
      "cg_created_time",
      "cg_dw_created_time",
      "cg_updated_time",
      "cg_dw_updated_time"
    )
    assert(df.columns.toSet === expectedColumns)
  }

}
