package com.alefeducation.dimensions.course.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.course.transform.CourseMutatedTransform._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class CourseMutatedTransformTest extends SparkSuite {

  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  def castTimestampColumns(df: Option[DataFrame]): Option[DataFrame] =
    df.map(_.withColumn("course_created_time", col("course_created_time").cast(TimestampType)))

  test("should construct course dataframe from published and updated") {
    val publishedValue =
      """
          |[
          |{
          | "eventType": "CoursePublishedEvent",
          |	"id": "course2",
          |	"organisation": "shared",
          | "courseType": "pathway",
          |	"name": "course2 name1",
          |	"createdOn": "2022-10-19T15:18:22.458",
          |	"code": "course2 code1",
          |	"subOrganisations": [
          |		"25e9b735-6b6c-403b-a9f3-95e478e8f1ed",
          |		"53055d6d-ecf6-4596-88db-0c50cac72cd0",
          |		"50445c7f-0f0d-45d4-aee1-26919a9b50f5"
          |	],
          |	"subjectId": 754325,
          |	"description": "course2 description",
          |	"goal": "course2 goal",
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
          |	"occurredOn": "2022-01-01 00:01:00.000",
          | "langCode": "EN_US",
          | "courseVersion": "1.0",
          |	"curriculums": [
          |		{"curriculumId": 1, "gradeId":1, "subjectId": 1},
          |		{"curriculumId": 2, "gradeId":2, "subjectId": 2}
          |	],
          | "configuration": {
          |   "programEnabled": true,
          |   "resourcesEnabled": false,
          |   "placementType": "BY_ABILITY_TEST"
          |}
          |}
          |]
        """.stripMargin

    val updatedValue =
      """
          |[
          |{
          | "eventType": "CourseSettingsUpdatedEvent",
          |	"id": "course2",
          |	"status": "PUBLISHED",
          |	"courseType": "PATHWAY",
          |	"name": "course2 name2",
          |	"code": "course2 code2",
          |	"subjectId": 754325,
          | "langCode": "EN_US",
          | "occurredOn": "2022-01-01 00:02:00.00",
          | "courseStatus": "PUBLISHED",
          |	"curriculums": [
          |		{"curriculumId": 1, "gradeId":1, "subjectId": 1},
          |		{"curriculumId": 2, "gradeId":2, "subjectId": 2}
          |	]
          |},
          |{
          | "eventType": "CourseDetailsUpdatedEvent",
          |	"id": "course2",
          |	"status": "DRAFT",
          |	"courseType": "PATHWAY",
          |	"name": "Test Events - 3",
          |	"code": "Test Events - 3",
          |	"subjectId": 754325,
          | "langCode": "EN_US",
          | "occurredOn": "2022-01-01 00:03:00.000",
          | "courseStatus": "DRAFT",
          |	"curriculums": [
          |		{"curriculumId": 1, "gradeId":1, "subjectId": 1},
          |		{"curriculumId": 2, "gradeId":2, "subjectId": 2}
          |	]
          |},
          |{
          | "eventType": "CourseDetailsUpdatedEvent",
          |	"id": "course2",
          |	"status": "PUBLISHED",
          |	"courseStatus": "PUBLISHED",
          |	"courseType": "PATHWAY",
          |	"name": "course2 name3",
          |	"code": "course2 code3",
          |	"subjectId": 754325,
          | "langCode": "EN_US",
          | "occurredOn": "2022-01-01 00:04:00.000",
          |	"curriculums": [
          |		{"curriculumId": 1, "gradeId":1, "subjectId": 1},
          |		{"curriculumId": 2, "gradeId":2, "subjectId": 2}
          |	]
          |},
          |{
          | "eventType": "CourseDetailsUpdatedEvent",
          |	"id": "course1",
          |	"status": "PUBLISHED",
          |	"courseStatus": "PUBLISHED",
          |	"courseType": "PATHWAY",
          |	"name": "course1 name2",
          |	"code": "course1 code2",
          |	"subjectId": 754325,
          | "langCode": "EN_US",
          | "occurredOn": "2022-01-01 00:05:00.000",
          |	"curriculums": [
          |		{"curriculumId": 1, "gradeId":1, "subjectId": 1},
          |		{"curriculumId": 2, "gradeId":2, "subjectId": 2}
          |	],
          |  "configuration": {
          |   "programEnabled": true,
          |   "resourcesEnabled": false,
          |   "placementType": "BY_ABILITY_TEST"
          |}
          |}
          |]
        """.stripMargin

    val deltaExistingValue =
      """
          |[
          |{
          | "course_id": "course1",
          | "course_name": "course1 name1",
          | "course_code": "course1 code1",
          | "course_organization": "org1",
          | "course_description": "description",
          | "course_goal": "main goal",
          | "course_status": 1,
          | "course_created_time": "2022-01-01 00:01:00.000",
          | "course_dw_created_time": "2022-01-01 00:02:00.000",
          | "course_dw_updated_time": null,
          | "course_updated_time": null,
          | "course_deleted_time": null,
          | "course_lang_code": "EN_US"
          |},
          |{
          | "course_id": "course1",
          | "course_name": "course1 name1 dup",
          | "course_code": "course1 code1",
          | "course_organization": "org1",
          | "course_description": "description",
          | "course_goal": "main goal",
          | "course_status": 1,
          | "course_created_time": "2022-01-01 00:01:00.000",
          | "course_dw_created_time": "2022-01-01 00:02:00.000",
          | "course_dw_updated_time": null,
          | "course_updated_time": null,
          | "course_deleted_time": null,
          | "course_lang_code": "EN_US"
          |},
          |{
          | "course_id": "course1",
          | "course_name": "course1 name1 dup",
          | "course_code": "course1 code1",
          | "course_organization": "org1",
          | "course_description": "description",
          | "course_goal": "main goal",
          | "course_status": 1,
          | "course_created_time": "2022-01-01 00:00:00.000",
          | "course_dw_created_time": "2022-01-01 00:02:00.000",
          | "course_dw_updated_time": null,
          | "course_updated_time": null,
          | "course_deleted_time": null,
          | "course_lang_code": "EN_US"
          |},
          |{
          | "course_id": "course1",
          | "course_name": "course1 name1",
          | "course_code": "course1 code1",
          | "course_organization": "org1 latest",
          | "course_description": "description",
          | "course_goal": "main goal",
          | "course_status": 1,
          | "course_created_time": "2022-01-01 00:04:00.000",
          | "course_dw_created_time": "2022-01-01 00:02:00.000",
          | "course_dw_updated_time": null,
          | "course_updated_time": null,
          | "course_deleted_time": null,
          | "course_lang_code": "EN_US"
          |}
          |]
        """.stripMargin

    val expectedColumns = Set(
      "course_id",
      "course_name",
      "course_code",
      "course_organization",
      "course_description",
      "course_goal",
      "course_status",
      "course_created_time",
      "course_updated_time",
      "course_dw_created_time",
      "course_dw_updated_time",
      "course_lang_code",
      "course_type",
      "rel_course_dw_id",
      "course_program_enabled",
      "course_resources_enabled",
      "course_placement_type"
    )

    val sprk = spark
    import sprk.implicits._

    val published = spark.read.json(Seq(publishedValue).toDS())
    when(
      service.readUniqueOptional(CoursePublishedParquetSource, sprk, uniqueColNames = List("id", "courseVersion"))
    ).thenReturn(Some(published))

    val updated = spark.read.json(Seq(updatedValue).toDS())
    when(
      service.readUniqueOptional(
        CourseUpdatedParquetSource,
        sprk,
        extraProps = List(("mergeSchema", "true")),
        uniqueColNames = List("id", "courseVersion")
      )
    ).thenReturn(Some(updated))

    val delta = spark.read.json(Seq(deltaExistingValue).toDS())
    when(service.readOptional(CourseDeltaSource, sprk)).thenReturn(castTimestampColumns(Some(delta)))

    when(service.getStartIdUpdateStatus("dim_course")).thenReturn(1)

    val transformer = new CourseMutatedTransform(sprk, service)
    val sinks = transformer.transform()

    val df = sinks.filter(_.get.name == CourseMutatedTransformedSink).head.get.output

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 4)

    val publishedDf = df.filter($"rel_course_dw_id" === 1)
    assert[String](publishedDf, "course_id", "course2")
    assert[String](publishedDf, "course_name", "course2 name1")
    assert[String](publishedDf, "course_code", "course2 code1")
    assert[String](publishedDf, "course_organization", "shared")
    assert[String](publishedDf, "course_description", "course2 description")
    assert[String](publishedDf, "course_goal", "course2 goal")
    assert[Int](publishedDf, "course_status", 2)
    assert[String](publishedDf, "course_created_time", "2022-01-01 00:01:00.0")
    assert[String](publishedDf, "course_updated_time", "2022-01-01 00:04:00.0")
    assert[String](publishedDf, "course_lang_code", "EN_US")
    assert[String](publishedDf, "course_type", "pathway")

    val updatedActiveDf = df.filter(($"course_status" === 1) && ($"course_id" === "course2"))
    assert[String](updatedActiveDf, "course_name", "course2 name3")
    assert[String](updatedActiveDf, "course_code", "course2 code3")
    assert[String](updatedActiveDf, "course_organization", "shared")
    assert[String](updatedActiveDf, "course_description", "course2 description")
    assert[String](updatedActiveDf, "course_goal", "course2 goal")
    assert[Int](updatedActiveDf, "course_status", 1)
    assert[String](updatedActiveDf, "course_created_time", "2022-01-01 00:04:00.0")
    assert[String](updatedActiveDf, "course_updated_time", null)
    assert[String](updatedActiveDf, "course_lang_code", "EN_US")

    val updatedExistingDf = df.filter($"course_id" === "course1")
    assert[String](updatedExistingDf, "course_name", "course1 name2")
    assert[String](updatedExistingDf, "course_code", "course1 code2")
    assert[String](updatedExistingDf, "course_organization", "org1 latest")
    assert[String](updatedExistingDf, "course_description", "description")
    assert[String](updatedExistingDf, "course_goal", "main goal")
    assert[Int](updatedExistingDf, "course_status", 1)
    assert[String](updatedExistingDf, "course_created_time", "2022-01-01 00:05:00.0")
    assert[String](updatedExistingDf, "course_updated_time", null)
    assert[String](updatedExistingDf, "course_lang_code", "EN_US")
  }

  test("should construct course dataframe from published") {
    val publishedValue =
      """
        |[
        |{
        | "eventType": "CoursePublishedEvent",
        |	"id": "course2",
        |	"organisation": "shared",
        | "courseType": "pathway",
        |	"name": "course2 name1",
        |	"createdOn": "2022-10-19T15:18:22.458",
        |	"code": "course2 code1",
        |	"subOrganisations": [
        |		"25e9b735-6b6c-403b-a9f3-95e478e8f1ed",
        |		"53055d6d-ecf6-4596-88db-0c50cac72cd0",
        |		"50445c7f-0f0d-45d4-aee1-26919a9b50f5"
        |	],
        |	"subjectId": 754325,
        |	"description": "course2 description",
        |	"goal": "course2 goal",
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
        |	"occurredOn": "2022-01-01 00:01:00.000",
        | "langCode": "EN_US",
        |	"curriculums": [
        |		{"curriculumId": 1, "gradeId":1, "subjectId": 1},
        |		{"curriculumId": 2, "gradeId":2, "subjectId": 2}
        |	],
        |  "configuration": {
        |   "programEnabled": true,
        |   "resourcesEnabled": false,
        |   "placementType": "BY_ABILITY_TEST"
        |}
        |}
        |]
        """.stripMargin

    val expectedColumns = Set(
      "course_id",
      "course_name",
      "course_code",
      "course_organization",
      "course_description",
      "course_goal",
      "course_status",
      "course_created_time",
      "course_updated_time",
      "course_dw_created_time",
      "course_dw_updated_time",
      "course_lang_code",
      "course_type",
      "rel_course_dw_id",
      "course_program_enabled",
      "course_resources_enabled",
      "course_placement_type"
    )

    val sprk = spark
    import sprk.implicits._

    val published = spark.read.json(Seq(publishedValue).toDS())
    when(
      service.readUniqueOptional(CoursePublishedParquetSource, sprk, uniqueColNames = List("id", "courseVersion"))
    ).thenReturn(Some(published))

    when(
      service.readUniqueOptional(
        CourseUpdatedParquetSource,
        sprk,
        extraProps = List(("mergeSchema", "true")),
        uniqueColNames = List("id", "courseVersion")
      )
    ).thenReturn(None)

    when(service.readOptional(CourseDeltaSource, sprk)).thenReturn(None)

    when(service.getStartIdUpdateStatus("dim_course")).thenReturn(1)

    val transformer = new CourseMutatedTransform(sprk, service)
    val sinks = transformer.transform()

    val df = sinks.filter(_.get.name == CourseMutatedTransformedSink).head.get.output

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 1)

    assert[Int](df, "rel_course_dw_id", 1)
    assert[String](df, "course_id", "course2")
    assert[String](df, "course_name", "course2 name1")
    assert[String](df, "course_code", "course2 code1")
    assert[String](df, "course_organization", "shared")
    assert[String](df, "course_description", "course2 description")
    assert[String](df, "course_goal", "course2 goal")
    assert[Int](df, "course_status", 1)
    assert[String](df, "course_created_time", "2022-01-01 00:01:00.0")
    assert[String](df, "course_updated_time", null)
    assert[String](df, "course_lang_code", "EN_US")
  }

  test("should construct course dataframe from updated and empty delta") {
    val publishedValue =
      """
        |[
        |{
        | "eventType": "CoursePublishedEvent",
        | "id": "course2",
        | "organisation": "shared",
        | "courseType": "pathway",
        | "name": "course2 name1",
        | "createdOn": "2022-10-19T15:18:22.458",
        | "code": "course2 code1",
        | "subOrganisations": [
        |		"25e9b735-6b6c-403b-a9f3-95e478e8f1ed",
        |		"53055d6d-ecf6-4596-88db-0c50cac72cd0",
        |		"50445c7f-0f0d-45d4-aee1-26919a9b50f5"
        | ],
        | "subjectId": 754325,
        | "description": "course2 description",
        | "goal": "course2 goal",
        | "modules": [
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
        | "occurredOn": "2022-01-01 00:01:00.000",
        | "langCode": "EN_US",
        |	"curriculums": [
        |		{"curriculumId": 1, "gradeId":1, "subjectId": 1},
        |		{"curriculumId": 2, "gradeId":2, "subjectId": 2}
        |	],
        |  "configuration": {
        |   "programEnabled": true,
        |   "resourcesEnabled": false,
        |   "placementType": "BY_ABILITY_TEST"
        |}
        |}
        |]
        """.stripMargin

    val updatedValue =
      """
        |[
        |{
        | "eventType": "CourseSettingsUpdatedEvent",
        | "id": "course2",
        | "status": "PUBLISHED",
        | "courseType": "PATHWAY",
        | "name": "course2 name2",
        | "code": "course2 code2",
        | "subjectId": 754325,
        | "langCode": "EN_US",
        | "occurredOn": "2022-01-01 00:02:00.00",
        | "courseStatus": "PUBLISHED",
        |	"curriculums": [
        |		{"curriculumId": 1, "gradeId":1, "subjectId": 1},
        |		{"curriculumId": 2, "gradeId":2, "subjectId": 2}
        |	],
        |  "configuration": {
        |   "programEnabled": true,
        |   "resourcesEnabled": false,
        |   "placementType": "BY_ABILITY_TEST"
        |}
        |}
        |]
        """.stripMargin

    val expectedColumns = Set(
      "course_id",
      "course_name",
      "course_code",
      "course_organization",
      "course_description",
      "course_goal",
      "course_status",
      "course_created_time",
      "course_updated_time",
      "course_dw_created_time",
      "course_dw_updated_time",
      "course_lang_code",
      "course_type",
      "rel_course_dw_id",
      "course_program_enabled",
      "course_resources_enabled",
      "course_placement_type"
    )

    val sprk = spark
    import sprk.implicits._

    val published = spark.read.json(Seq(publishedValue).toDS())
    when(
      service.readUniqueOptional(CoursePublishedParquetSource, sprk, uniqueColNames = List("id", "courseVersion"))
    ).thenReturn(Some(published))

    val updated = spark.read.json(Seq(updatedValue).toDS())
    when(
      service.readUniqueOptional(
        CourseUpdatedParquetSource,
        sprk,
        extraProps = List(("mergeSchema", "true")),
        uniqueColNames = List("id", "courseVersion")
      )
    ).thenReturn(Some(updated))

    when(service.readOptional(CourseDeltaSource, sprk)).thenReturn(None)

    when(service.getStartIdUpdateStatus("dim_course")).thenReturn(1)

    val transformer = new CourseMutatedTransform(sprk, service)
    val sinks = transformer.transform()

    val df = sinks.filter(_.get.name == CourseMutatedTransformedSink).head.get.output

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 2)

    val updatedDf = df.filter($"rel_course_dw_id" === 2)
    assert[String](updatedDf, "course_id", "course2")
    assert[String](updatedDf, "course_name", "course2 name2")
    assert[String](updatedDf, "course_code", "course2 code2")
    assert[String](updatedDf, "course_organization", "shared")
    assert[String](updatedDf, "course_description", "course2 description")
    assert[String](updatedDf, "course_goal", "course2 goal")
    assert[Int](updatedDf, "course_status", 1)
    assert[String](updatedDf, "course_created_time", "2022-01-01 00:02:00.0")
    assert[String](updatedDf, "course_updated_time", null)
    assert[String](updatedDf, "course_lang_code", "EN_US")
  }

  test("should construct course dataframe from updated and delta") {
    val updatedValue =
      """
        |[
        |{
        |   "eventType": "CourseDetailsUpdatedEvent",
        |	"id": "course1",
        |	"status": "PUBLISHED",
        |	"courseStatus": "PUBLISHED",
        |	"courseType": "PATHWAY",
        |	"name": "course1 name2",
        |	"code": "course1 code2",
        |	"subjectId": 754325,
        |   "langCode": "EN_US",
        |   "occurredOn": "2022-01-01 00:05:00.000",
        |	"curriculums": [
        |		{"curriculumId": 1, "gradeId":1, "subjectId": 1},
        |		{"curriculumId": 2, "gradeId":2, "subjectId": 2}
        |	],
        |  "configuration": {
        |   "programEnabled": true,
        |   "resourcesEnabled": false,
        |   "placementType": "BY_ABILITY_TEST"
        |}
        |}
        |]
        """.stripMargin

    val deltaExistingValue =
      """
        |[
        |{
        | "course_id": "course1",
        | "course_name": "course1 name1",
        | "course_code": "course1 code1",
        | "course_organization": "org1",
        | "course_description": "description",
        | "course_goal": "main goal",
        | "course_status": 1,
        | "course_created_time": "2022-01-01 00:01:00.000",
        | "course_dw_created_time": "2022-01-01 00:02:00.000",
        | "course_dw_updated_time": null,
        | "course_updated_time": null,
        | "course_deleted_time": null,
        | "course_lang_code": "EN_US"
        |}
        |]
        """.stripMargin

    val expectedColumns = Set(
      "course_id",
      "course_name",
      "course_code",
      "course_organization",
      "course_description",
      "course_goal",
      "course_status",
      "course_created_time",
      "course_updated_time",
      "course_dw_created_time",
      "course_dw_updated_time",
      "course_lang_code",
      "course_type",
      "rel_course_dw_id",
      "course_program_enabled",
      "course_resources_enabled",
      "course_placement_type"
    )

    val sprk = spark
    import sprk.implicits._

    when(
      service.readUniqueOptional(CoursePublishedParquetSource, sprk, uniqueColNames = List("id", "courseVersion"))
    ).thenReturn(None)

    val updated = spark.read.json(Seq(updatedValue).toDS())
    when(
      service.readUniqueOptional(
        CourseUpdatedParquetSource,
        sprk,
        extraProps = List(("mergeSchema", "true")),
        uniqueColNames = List("id", "courseVersion")
      )
    ).thenReturn(Some(updated))

    val delta = spark.read.json(Seq(deltaExistingValue).toDS())
    when(service.readOptional(CourseDeltaSource, sprk)).thenReturn(castTimestampColumns(Some(delta)))

    when(service.getStartIdUpdateStatus("dim_course")).thenReturn(1)

    val transformer = new CourseMutatedTransform(sprk, service)
    val sinks = transformer.transform()

    val df = sinks.filter(_.get.name == CourseMutatedTransformedSink).head.get.output

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 1)

    assert[String](df, "course_id", "course1")
    assert[String](df, "course_name", "course1 name2")
    assert[String](df, "course_code", "course1 code2")
    assert[String](df, "course_organization", "org1")
    assert[String](df, "course_description", "description")
    assert[String](df, "course_goal", "main goal")
    assert[Int](df, "course_status", 1)
    assert[String](df, "course_created_time", "2022-01-01 00:05:00.0")
    assert[String](df, "course_updated_time", null)
    assert[String](df, "course_lang_code", "EN_US")
  }

}
