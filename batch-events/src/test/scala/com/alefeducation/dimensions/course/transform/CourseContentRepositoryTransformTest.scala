package com.alefeducation.dimensions.course.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.course.transform.CourseContentRepositoryTransform.{ContentRepositoryMaterialAttachSource, ContentRepositoryMaterialDetachSource, CourseContentRepositoryTransformedSink, CoursePublishedParquetSource}
import com.alefeducation.util.Resources.ALEF_39389_PATHWAY_CONTENT_REPO_IWH_TOGGLE
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class CourseContentRepositoryTransformTest extends SparkSuite {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  test("should construct course content repo dataframe from published") {
    val value =
      """
        |[
        |{
        | "eventType": "CoursePublishedEvent",
        |	"id": "88fc9cea-85af-45dd-a8a7-43ef7bf5f598",
        |	"organisation": "shared",
        |	"name": "Test Events - 2",
        |	"code": "Test Events - 1",
        | "courseType": "PATHWAY",
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
        |	]
        |},
        |{
        | "eventType": "CoursePublishedEvent",
        |	"id": "88fc9cea-85af-45dd-a8a7-43ef7bf5f598",
        |	"organisation": "shared",
        |	"name": "Test Events - 2",
        |	"code": "Test Events - 1",
        | "courseType": "PATHWAY",
        |	"subOrganisations": [
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
        |	"occurredOn": "2021-06-23 06:33:24.921",
        | "langCode": "EN_US",
        |	"curriculums": [
        |		{"curriculumId": 1, "gradeId":1, "subjectId": 1}
        |	]
        |}
        |]
        """.stripMargin

    val contentRepoMaterialAttachEvent =
      """
        |[
        |{
        | "eventType": "ContentRepositoryMaterialAttachedEvent",
        | "id": "975afcba-e214-4d3c-a84a-000000234546",
        |  "organisation": "shared",
        |  "occurredOn":"2021-06-23 06:33:24.921",
        |  "updatedAt":1573959530000,
        |  "updatedById": 1234,
        |  "updatedByName": "admin",
        |  "addedMaterials": [
        |    {
        |      "id":"985afcba-e214-4d3c-a84a-120000234567",
        |      "type": "CORE"
        |     },
        |     {
        |      "id":"985afcba-edf14-4d3c-a84a-120000234567",
        |      "type": "PATHWAY"
        |     },
        |    {
        |      "id": "995afcba-e214-4d3c-a84a-023000234568",
        |      "type": "InstructionalPlan"
        |    }
        |  ]
        |}
        |]
        """.stripMargin

    val contentRepoMaterialDetachEvent =
      """
        |[
        |{
        | "eventType": "ContentRepositoryMaterialDetachedEvent",
        | "id": "975afcba-e214-4d3c-a84a-000000234546",
        |  "organisation": "shared",
        |  "occurredOn":"2021-06-23 06:33:24.921",
        |  "updatedAt":1573959530000,
        |  "updatedById": 1234,
        |  "updatedByName": "admin",
        |  "removedMaterials": [
        |    {
        |      "id": "985afcba-e214-4d3c-a84a-120000234567",
        |      "type": "CORE"
        |     },
        |    {
        |      "id": "995afcba-e214-4d3c-a84a-023000234568",
        |      "type": "InstructionalPlan"
        |    }
        |  ]
        |}
        |]
        """.stripMargin
    val sprk = spark
    import sprk.implicits._

    val inputDF = spark.read.json(Seq(value).toDS())
    val contentRepoMaterialAttachDf = spark.read.json(Seq(contentRepoMaterialAttachEvent).toDS())
    val contentRepoMaterialDetachDf = spark.read.json(Seq(contentRepoMaterialDetachEvent).toDS())
    when(service.readOptional(CoursePublishedParquetSource, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(inputDF))
    when(service.readOptional(ContentRepositoryMaterialAttachSource, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(contentRepoMaterialAttachDf))
    when(service.readOptional(ContentRepositoryMaterialDetachSource, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(contentRepoMaterialDetachDf))

    val transformer = new CourseContentRepositoryTransform(sprk, service)
    val sinks = transformer.transform()

    val df = sinks.filter(_.name == CourseContentRepositoryTransformedSink).head.output

      assert(df.count() == 9)
      val expectedColumns = Set(
        "ccr_dw_updated_time", "ccr_dw_id", "ccr_updated_time", "ccr_repository_id", "ccr_created_time", "ccr_course_id", "ccr_status", "ccr_course_type", "ccr_dw_created_time", "ccr_attach_status"
      )
      assert(df.columns.toSet === expectedColumns)
      assert[String](df, "ccr_course_id", "88fc9cea-85af-45dd-a8a7-43ef7bf5f598")
      assert[String](df, "ccr_repository_id", "25e9b735-6b6c-403b-a9f3-95e478e8f1ed")
      assert[Int](df, "ccr_status", 1)
      assert[String](df, "ccr_created_time", "2021-06-23 05:33:24.921")
  }

  test("should construct course content repo dataframe from published when content repo is empty") {
    val value =
      """
        |[
        |{
        | "eventType": "CoursePublishedEvent",
        |	"id": "88fc9cea-85af-45dd-a8a7-43ef7bf5f598",
        |	"organisation": "shared",
        |	"name": "Test Events - 2",
        | "courseType": "PATHWAY",
        |	"createdOn": "2022-10-19T15:18:22.458",
        |	"code": "Test Events - 1",
        |	"subOrganisations": [],
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
        |	]
        |}
        |]
        """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val inputDF = spark.read.json(Seq(value).toDS())
    when(service.readOptional(CoursePublishedParquetSource, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(inputDF))
    when(service.readOptional(ContentRepositoryMaterialAttachSource, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(None)
    when(service.readOptional(ContentRepositoryMaterialDetachSource, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(None)

    val transformer = new CourseContentRepositoryTransform(sprk, service)
    val sinks = transformer.transform()

      val expectedColumns = Set(
        "ccr_dw_updated_time", "ccr_dw_id", "ccr_updated_time", "ccr_repository_id", "ccr_created_time", "ccr_course_id", "ccr_status", "ccr_course_type", "ccr_dw_created_time", "ccr_attach_status"
      )
      val df = sinks.filter(_.name == CourseContentRepositoryTransformedSink).head.output
      assert(df.columns.toSet === expectedColumns)
      assert(sinks.size === 1)

      assert[String](df, "ccr_course_id", "88fc9cea-85af-45dd-a8a7-43ef7bf5f598")
      assert[String](df, "ccr_repository_id", null)
      assert[Int](df, "ccr_status", 1)
      assert[String](df, "ccr_created_time", "2021-06-23 05:33:24.921")
  }
}
