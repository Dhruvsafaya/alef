package com.alefeducation.dimensions.course.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.course.transform.CourseDwIdMappingTransform.CourseDwIdMappingSink
import com.alefeducation.dimensions.course.transform.CourseMutatedTransform.CoursePublishedParquetSource
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class CourseDwIdMappingTransformTest extends SparkSuite {

  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  test("should construct course dw id mapping dataframe") {
    val value =
      """
        |[
        |{
        |   "eventType": "CoursePublishedEvent",
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
        |	"occurredOn": 1666193339145,
        |	"langCode": "EN_US"
        |}
        |]
        """.stripMargin
    val sprk = spark
    import sprk.implicits._
    val transformer = new CourseDwIdMappingTransform(sprk, service)
    val inputDF = spark.read.json(Seq(value).toDS())
    when(service.readOptional(CoursePublishedParquetSource, sprk)).thenReturn(Some(inputDF))

    val sinks = transformer.transform()
    val df = sinks.filter(_.get.name == CourseDwIdMappingSink).head.get.output

    assert[String](df, "id", "88fc9cea-85af-45dd-a8a7-43ef7bf5f598")
    assert[String](df, "entity_type", "course")
  }

}
