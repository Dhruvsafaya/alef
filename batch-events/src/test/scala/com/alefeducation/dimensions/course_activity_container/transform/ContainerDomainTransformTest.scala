package com.alefeducation.dimensions.course_activity_container.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.course_activity_container.transform.ContainerDomainTransform.{ContainerDomainKey, ContainerDomainTransformService}
import com.alefeducation.dimensions.course_activity_container.transform.ContainerDwIdMappingTransform.ContainerAddedParquetSource
import com.alefeducation.dimensions.course_activity_container.transform.ContainerMutatedTransform.{ContainerPublishedParquetSource, ContainerUpdatedParquetSource}
import com.alefeducation.dimensions.course_activity_container.transform.ContainerTransformUtils.addMetadata
import com.alefeducation.util.Resources.{getSink, getSource}
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class ContainerDomainTransformTest extends SparkSuite {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  val expectedColumns = Set(
    "cacd_updated_time",
    "cacd_course_id",
    "cacd_status",
    "cacd_domain",
    "cacd_sequence",
    "cacd_container_id",
    "cacd_dw_updated_time",
    "cacd_created_time",
    "cacd_dw_created_time",
    "cacd_dw_id"
  )

  test("should construct course activity container domain association dataframe") {
    val publishedValue =
      """
        |[
        |  {
        | "eventType": "ContainerPublishedWithCourseEvent",
        |	"id": "level1_id",
        |	"courseId": "pathwayId",
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
        |			"id": "a0b1bb0a-56c1-4bbe-b35f-000000028393",
        |			"settings": {
        |				"pacing": "UN_LOCKED",
        |				"hideWhenPublishing": false,
        |				"isOptional": false
        |			},
        |			"type": "ACTIVITY"
        |		},
        |		{
        |			"id": "069ccfdb-0c63-46c9-9c4e-f35f8512e66d",
        |			"settings": {
        |				"pacing": "UN_LOCKED",
        |				"hideWhenPublishing": false,
        |				"isOptional": false
        |			},
        |			"type": "INTERIM_CHECKPOINT"
        |		}
        |	],
        |	"longName": "Phantoms long name",
        |	"description": "Level description",
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
        | "courseVersion": "228",
        |	"occurredOn": "2021-06-23 05:33:24.921",
        | "isAccelerated": false
        |}]
        """.stripMargin

    val metadataPublishedValue =
      """{
        |   "tags": [
        |     {
        |       "key": "Grade",
        |       "values": ["1"],
        |       "type": "LIST"
        |     },
        |     {
        |       "key": "Domain",
        |       "values": [
        |         "{\"name\": \"Geometry\",\"icon\": \"Geometry\",\"color\": \"lightBlue\"}",
        |         "{\"name\": \"Algebra and Algebraic Thinking\",\"icon\": \"AlgebraAndAlgebraicThinking\",\"color\": \"lightGreen\"}"
        |       ],
        |       "attributes": [
        |         {"value": "val01", "color": "green", "translation": "tr01"}
        |       ],
        |       "type": "DOMAIN"
        |     }
        |   ]
        |}""".stripMargin

    val addedValue =
      """
        |[
        | {
        | "eventType": "ContainerAddedInCourseEvent",
        |	"id": "level2_id",
        |	"courseId": "pathwayId",
        |	"index": 0,
        |	"title": "Level-2",
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
        | "sequences": [
        |		{
        |			"domain": "Math",
        |			"sequence": 2
        |		},
        |		{
        |			"domain": "Algebra and Algebraic Thinking",
        |			"sequence": 3
        |		}
        |	],
        | "courseVersion": "228",
        |	"occurredOn": "2021-06-23 06:33:24.921",
        | "isAccelerated":false
        | }
        |]
        |""".stripMargin

    val metadataAddValue =
      """
        |{
        |   "tags": [
        |     {
        |       "key": "Grade",
        |       "values": ["1"],
        |       "type": "LIST"
        |     },
        |     {
        |       "key": "Domain",
        |       "values": [
        |         "{\"name\": \"Geometry\",\"icon\": \"Geometry\",\"color\": \"lightBlue\"}",
        |         "{\"name\": \"Algebra and Algebraic Thinking\",\"icon\": \"AlgebraAndAlgebraicThinking\",\"color\": \"lightGreen\"}"
        |       ],
        |       "attributes": [
        |         {"value": "val01", "color": "green", "translation": "tr01"}
        |       ],
        |       "type": "DOMAIN"
        |     }
        |   ]
        |}
        |""".stripMargin

    val updatedValue =
      """
        |[
        | {
        | "eventType": "ContainerUpdatedInCourseEvent",
        |	"id": "level2_id",
        |	"courseId": "pathwayId",
        |	"index": 0,
        |	"title": "Level-22",
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
        |	"sequences": [
        |		{
        |			"domain": "English",
        |			"sequence": 1
        |		}
        |	],
        | "courseVersion": "228",
        |	"occurredOn": "2021-06-23 07:33:24.921"
        | }
        |]
        |""".stripMargin

    val metadataUpdatedValue =
      """
        |{
        |   "tags": [
        |     {
        |       "key": "Grade",
        |       "values": ["1", "2"],
        |       "type": "LIST"
        |     },
        |     {
        |       "key": "Domain",
        |       "values": [
        |         "{\"name\": \"Geometry\",\"icon\": \"Geometry\",\"color\": \"lightBlue\"}"
        |       ],
        |       "attributes": [
        |         {"value": "val01", "color": "green", "translation": "tr01"}
        |       ],
        |       "type": "DOMAIN"
        |     }
        |   ]
        | }
        |""".stripMargin

    val sprk = spark
    import sprk.implicits._

    val publishedInput = spark.read.json(Seq(publishedValue).toDS()).transform(addMetadata(metadataPublishedValue))
    when(service.readOptional(ContainerPublishedParquetSource, sprk)).thenReturn(Some(publishedInput))

    val addedInput = spark.read.json(Seq(addedValue).toDS()).transform(addMetadata(metadataAddValue))
    when(service.readOptional(ContainerAddedParquetSource, sprk)).thenReturn(Some(addedInput))
    val updatedInput = spark.read.json(Seq(updatedValue).toDS()).transform(addMetadata(metadataUpdatedValue))
    when(service.readOptional(ContainerUpdatedParquetSource, sprk)).thenReturn(Some(updatedInput))


    when(service.getStartIdUpdateStatus(ContainerDomainKey)).thenReturn(1001)

    val transformer = new ContainerDomainTransform(sprk, service)

    val sourceNames = getSource(ContainerDomainTransformService)
    val sinkName = getSink(ContainerDomainTransformService).head

    val sink = transformer.transform(sourceNames, sinkName)

    val df = sink.get.output

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() === 5)
  }
}
