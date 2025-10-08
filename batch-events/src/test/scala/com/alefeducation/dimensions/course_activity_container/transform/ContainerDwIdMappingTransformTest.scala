package com.alefeducation.dimensions.course_activity_container.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.course_activity_container.transform.ContainerDwIdMappingTransform.{ContainerAddedParquetSource, ContainerDwIdMappingSink}
import com.alefeducation.dimensions.course_activity_container.transform.ContainerMutatedTransform.ContainerPublishedParquetSource
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock


class ContainerDwIdMappingTransformTest extends SparkSuite {

  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  test("should construct pathway level dw id mapping dataframe") {
    val publishedValue =
      """
        |[
        | {
        | "eventType": "ContainerPublishedWithCourseEvent",
        |	"id": "12374750-2b0e-4279-9788-def1007575e8",
        |	"courseId": "88fc9cea-85af-45dd-a8a7-43ef7bf5f598",
        |	"index": 0,
        |	"title": "Level-1",
        |	"settings": {
        |		"pacing": "SELF_PACED",
        |		"hideWhenPublishing": false,
        |		"isOptional": false
        |	},
        |	"items": [
        |		{
        |			"id": "03583155-b890-4ab6-a3c4-000000028179",
        |			"settings": {
        |				"pacing": "UN_LOCKED",
        |				"hideWhenPublishing": false,
        |				"isOptional": false
        |			},
        |			"type": "ACTIVITY"
        |		},
        |		{
        |			"id": "5c0bbf43-bc98-49d7-93ae-000000028400",
        |			"settings": {
        |				"pacing": "UN_LOCKED",
        |				"hideWhenPublishing": false,
        |				"isOptional": false
        |			},
        |			"type": "ACTIVITY"
        |	  }
        |	],
        |	"longName": "Phantoms long name",
        |	"description": "Level description",
        |	"metadata": "{\"tags\": [{\"key\": \"Grade\",\"values\": [\"KG\",\"1\"],\"type\": \"LIST\"},{\"key\": \"Domain\",\"values\": [{\"name\": \"Geometry\",\"icon\": \"Geometry\",\"color\": \"lightBlue\"},{\"name\": \"Algebra and Algebraic Thinking\",\"icon\": \"AlgebraAndAlgebraicThinking\",\"color\": \"lightGreen\"}],\"type\": \"DOMAIN\"}]}",
        |	"sequences": [
        |		{
        |			"domain": "Geometry",
        |			"sequence": 1
        |		},
        |		{
        |			"domain": "Algebra and Algebraic Thinking",
        |			"sequence": 1
        |		}
        |	],
        |	"occurredOn": "2021-06-23 05:33:24.921"
        |}
        |]
        """.stripMargin

    val addedValue =
      """
        |[
        | {
        | "eventType": "ContainerAddedInCourseEvent",
        |	"id": "00000000-2b0e-4279-9788-def100000000",
        |	"pathwayId": "88fc9cea-85af-45dd-a8a7-43ef7bf5f598",
        |	"index": 0,
        |	"title": "Level-1",
        |	"settings": {
        |		"pacing": "SELF_PACED",
        |		"hideWhenPublishing": false,
        |		"isOptional": false
        |	},
        |	"items": [
        |		{
        |			"id": "03583155-b890-4ab6-a3c4-000000028179",
        |			"settings": {
        |				"pacing": "UN_LOCKED",
        |				"hideWhenPublishing": false,
        |				"isOptional": false
        |			},
        |			"type": "ACTIVITY"
        |		},
        |		{
        |			"id": "5c0bbf43-bc98-49d7-93ae-000000028400",
        |			"settings": {
        |				"pacing": "UN_LOCKED",
        |				"hideWhenPublishing": false,
        |				"isOptional": false
        |			},
        |			"type": "ACTIVITY"
        |	  }
        |	],
        |	"longName": "Phantoms long name",
        |	"description": "Level description",
        |	"metadata": "{\"tags\": [{\"key\": \"Grade\",\"values\": [\"KG\",\"1\"],\"type\": \"LIST\"},{\"key\": \"Domain\",\"values\": [{\"name\": \"Geometry\",\"icon\": \"Geometry\",\"color\": \"lightBlue\"},{\"name\": \"Algebra and Algebraic Thinking\",\"icon\": \"AlgebraAndAlgebraicThinking\",\"color\": \"lightGreen\"}],\"type\": \"DOMAIN\"}]}",
        |	"sequences": [
        |		{
        |			"domain": "Geometry",
        |			"sequence": 1
        |		},
        |		{
        |			"domain": "Algebra and Algebraic Thinking",
        |			"sequence": 1
        |		}
        |	],
        |	"occurredOn": "2021-06-23 05:33:24.921"
        | }
        |]
        |""".stripMargin

    val expectedColumns = Set(
      "id", "entity_created_time", "entity_dw_created_time", "entity_type"
    )

    val sprk = spark
    import sprk.implicits._
    val transformer = new ContainerDwIdMappingTransform(sprk, service)

    val publishedInput = spark.read.json(Seq(publishedValue).toDS())
    when(service.readOptional(ContainerPublishedParquetSource, sprk)).thenReturn(Some(publishedInput))

    val addedInput = spark.read.json(Seq(addedValue).toDS())
    when(service.readOptional(ContainerAddedParquetSource, sprk)).thenReturn(Some(addedInput))

    val sinks = transformer.transform()
    val df = sinks.filter(_.get.name == ContainerDwIdMappingSink).head.get.output

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 2)

    val firstDf = df.filter($"id" === "12374750-2b0e-4279-9788-def1007575e8")
    assert(!firstDf.isEmpty)
    assert[String](firstDf, "entity_type", "course_activity_container")

    val secondDf = df.filter($"id" === "00000000-2b0e-4279-9788-def100000000")
    assert(!firstDf.isEmpty)
    assert[String](firstDf, "entity_type", "course_activity_container")

  }


}
