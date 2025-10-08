package com.alefeducation.dimensions.course.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.course.transform.CourseCurriculumMutatedTransform.{
  CourseCurriculumMutatedTransformedSink,
  CourseDetailsUpdatedParquetSource
}
import com.alefeducation.dimensions.course.transform.CourseMutatedTransform.{CoursePublishedParquetSource, CourseUpdatedParquetSource}
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class CourseCurriculumTransformTest extends SparkSuite {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  test("should construct course curriculum dataframe from published event") {

    val expectedColumns = Set(
      "cc_course_id",
      "cc_curr_id",
      "cc_curr_grade_id",
      "cc_curr_subject_id",
      "cc_status",
      "cc_created_time",
      "cc_updated_time",
      "cc_dw_created_time",
      "cc_dw_updated_time",
      "cc_dw_id"
    )

    val sprk = spark
    import sprk.implicits._

    val value =
      """
        |[
        |{
        | "eventType": "CoursePublishedEvent",
        |	"id": "88fc9cea-85af-45dd-a8a7-43ef7bf5f598",
        |	"organisation": "shared",
        |	"name": "Test Events - 2",
        |	"createdOn": "2022-10-19T15:18:22.458",
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
        |	"occurredOn": "2021-06-23 00:33:24.921",
        | "langCode": "EN_US",
        |	"curriculums": [
        |		{"curriculumId": 1, "gradeId":1, "subjectId": 1},
        |		{"curriculumId": 2, "gradeId":2, "subjectId": 2}
        |	]
        |},
        |{
        | "eventType": "CoursePublishedEvent",
        |	"id": "88fc9cea-85af-45dd-a8a7-43ef7bf5f598",
        |	"organisation": "shared",
        |	"name": "Test Events - 2",
        |	"createdOn": "2022-10-19T15:18:22.458",
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
        |	"occurredOn": "2021-06-23 00:33:24.921",
        | "langCode": "EN_US",
        |	"curriculums": []
        |}
        |]
        """.stripMargin
    val inputDF = spark.read.json(Seq(value).toDS())
    val emptyDF = spark.emptyDataFrame

    when(service.readOptional(CoursePublishedParquetSource, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(inputDF))
    when(service.readOptional(CourseUpdatedParquetSource, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(emptyDF))

    val transformer = new CourseCurriculumMutatedTransform(sprk, service)
    val sinks = transformer.transform()

    val df = sinks.filter(_.name == CourseCurriculumMutatedTransformedSink).head.output

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 2)

    assert[String](df, "cc_course_id", "88fc9cea-85af-45dd-a8a7-43ef7bf5f598")
    assert[Int](df, "cc_curr_id", 1)
    assert[Int](df, "cc_curr_subject_id", 1)
    assert[Int](df, "cc_curr_grade_id", 1)
    assert[Int](df, "cc_status", 1)
    assert[String](df, "cc_created_time", "2021-06-23 00:33:24.921")
  }

  test("should construct course curriculum dataframe from published event when curriculum has empty array") {

    val expectedColumns = Set(
      "cc_course_id",
      "cc_curr_id",
      "cc_curr_grade_id",
      "cc_curr_subject_id",
      "cc_status",
      "cc_created_time",
      "cc_updated_time",
      "cc_dw_created_time",
      "cc_dw_updated_time",
      "cc_dw_id"
    )

    val sprk = spark
    import sprk.implicits._

    val value =
      """
        |[
        |{
        | "eventType": "CoursePublishedEvent",
        |	"id": "88fc9cea-85af-45dd-a8a7-43ef7bf5f598",
        |	"organisation": "shared",
        |	"name": "Test Events - 2",
        |	"createdOn": "2022-10-19T15:18:22.458",
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
        |	"occurredOn": "2021-06-23 00:33:24.921",
        | "langCode": "EN_US",
        |	"curriculums": []
        |}
        |]
        """.stripMargin
    val inputDF = spark.read.json(Seq(value).toDS())

    when(service.readOptional(CoursePublishedParquetSource, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(inputDF))

    val transformer = new CourseCurriculumMutatedTransform(sprk, service)
    val sinks = transformer.transform()
    assert(sinks.size === 0)
  }

  test("should construct course dataframe from course details updated event") {

    val expectedColumns = Set(
      "cc_course_id",
      "cc_curr_id",
      "cc_curr_grade_id",
      "cc_curr_subject_id",
      "cc_status",
      "cc_created_time",
      "cc_updated_time",
      "cc_dw_created_time",
      "cc_dw_updated_time",
      "cc_dw_id"
    )

    val sprk = spark
    import sprk.implicits._

    val value2 =
      """
        |[
        |{
        | "eventType": "CourseSettingsUpdatedEvent",
        |	"id": "88fc9cea-85af-45dd-a8a7-43ef7bf5f598",
        |	"status": "PUBLISHED",
        |	"type": "PATHWAY",
        |	"name": "Test Events - 2",
        |	"code": "Test Events - 1",
        |	"subjectId": 754325,
        | "langCode": "EN_US",
        | "occurredOn": "2022-06-23 05:33:24.921",
        | "courseStatus": "PUBLISHED",
        |	"curriculums": [
        |		{"curriculumId": 1, "gradeId":1, "subjectId": 1},
        |		{"curriculumId": 2, "gradeId":2, "subjectId": 2}
        |	]
        |}
        |]
        """.stripMargin
    val updatedDF = spark.read.json(Seq(value2).toDS())

    when(service.readOptional(CourseDetailsUpdatedParquetSource, sprk, extraProps = List(("mergeSchema", "true"))))
      .thenReturn(Some(updatedDF))

    val transformer = new CourseCurriculumMutatedTransform(sprk, service)
    val sinks = transformer.transform()

    val df = sinks.filter(_.name == CourseCurriculumMutatedTransformedSink).head.output

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 2)

    assert[String](df, "cc_course_id", "88fc9cea-85af-45dd-a8a7-43ef7bf5f598")
    assert[Int](df, "cc_curr_id", 1)
    assert[Int](df, "cc_curr_subject_id", 1)
    assert[Int](df, "cc_curr_grade_id", 1)
    assert[Int](df, "cc_status", 1)
    assert[String](df, "cc_created_time", "2022-06-23 05:33:24.921")
  }

  test("should filter dataframe when courseDetailsUpdatedEvent coming with courseStatus as Draft") {

    val expectedColumns = Set(
      "cc_course_id",
      "cc_curr_id",
      "cc_curr_grade_id",
      "cc_curr_subject_id",
      "cc_status",
      "cc_created_time",
      "cc_updated_time",
      "cc_dw_created_time",
      "cc_dw_updated_time",
      "cc_dw_id"
    )

    val sprk = spark
    import sprk.implicits._

    val value2 =
      """
        |[
        |{
        | "eventType": "CourseDetailsUpdatedEvent",
        |	"id": "88fc9cea-85af-45dd-a8a7-43ef7bf5f598",
        |	"status": "DRAFT",
        |	"type": "PATHWAY",
        |	"name": "Test Events - 2",
        |	"code": "Test Events - 1",
        |	"subjectId": 754325,
        | "langCode": "EN_US",
        | "occurredOn": "2022-06-23 05:33:24.921",
        | "courseStatus": "DRAFT",
        |	"curriculums": [
        |		{"curriculumId": 1, "gradeId":1, "subjectId": 1},
        |		{"curriculumId": 2, "gradeId":2, "subjectId": 2}
        |	]
        |}
        |]
        """.stripMargin
    val updatedDF = spark.read.json(Seq(value2).toDS())

    when(service.readOptional(CourseDetailsUpdatedParquetSource, sprk, extraProps = List(("mergeSchema", "true"))))
      .thenReturn(Some(updatedDF))

    val transformer = new CourseCurriculumMutatedTransform(sprk, service)
    val sinks = transformer.transform()

    assert(!sinks.exists(_.name == CourseCurriculumMutatedTransformedSink), true)
  }

  test("should construct course dataframe from course details updated event when curriculums is empty") {

    val expectedColumns = Set(
      "cc_course_id",
      "cc_curr_id",
      "cc_curr_grade_id",
      "cc_curr_subject_id",
      "cc_status",
      "cc_created_time",
      "cc_updated_time",
      "cc_dw_created_time",
      "cc_dw_updated_time",
      "cc_dw_id"
    )

    val sprk = spark
    import sprk.implicits._

    val value2 =
      """
        |[
        |{
        | "eventType": "CourseDetailsUpdatedEvent",
        |	"id": "88fc9cea-85af-45dd-a8a7-43ef7bf5f598",
        |	"status": "PUBLISHED",
        |	"type": "PATHWAY",
        |	"name": "Test Events - 2",
        |	"code": "Test Events - 1",
        |	"subjectId": 754325,
        | "langCode": "EN_US",
        | "occurredOn": "2022-06-23 05:33:24.921",
        | "courseStatus": "PUBLISHED",
        |	"curriculums": []
        |}
        |]
        """.stripMargin
    val updatedDF = spark.read.json(Seq(value2).toDS())

    when(service.readOptional(CourseDetailsUpdatedParquetSource, sprk, extraProps = List(("mergeSchema", "true"))))
      .thenReturn(Some(updatedDF))

    val transformer = new CourseCurriculumMutatedTransform(sprk, service)
    val sinks = transformer.transform()
    val df = sinks.filter(_.name == CourseCurriculumMutatedTransformedSink).head.output
    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 1)
    assert(df.schema("cc_curr_id").dataType.typeName == "long")
    assert(df.schema("cc_curr_subject_id").dataType.typeName == "long")
    assert(df.schema("cc_curr_grade_id").dataType.typeName == "long")
    assert[String](df, "cc_course_id", "88fc9cea-85af-45dd-a8a7-43ef7bf5f598")
    assert[String](df, "cc_curr_id", null)
    assert[String](df, "cc_curr_subject_id", null)
    assert[String](df, "cc_curr_grade_id", null)
    assert[Int](df, "cc_status", 1)
    assert[String](df, "cc_created_time", "2022-06-23 05:33:24.921")
  }
}
