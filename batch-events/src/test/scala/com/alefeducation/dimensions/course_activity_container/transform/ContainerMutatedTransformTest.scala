package com.alefeducation.dimensions.course_activity_container.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.course_activity_container.transform.ContainerMutatedTransform._
import com.alefeducation.dimensions.course_activity_container.transform.ContainerTransformUtils.addMetadata
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock


class ContainerMutatedTransformTest extends SparkSuite {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  test("should construct activity container dataframe") {
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
        |			"domain": "Geometry",
        |			"sequence": 1
        |		},
        |		{
        |			"domain": "Algebra and Algebraic Thinking",
        |			"sequence": 1
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
        |			"domain": "Geometry",
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

    val deletedValue =
      """
        |[
        |{
        | "eventType": "ContainerDeletedFromCourseEvent",
        |	"id": "level2_id",
        |	"courseId": "pathwayId",
        |	"index": 0,
        |	"courseVersion": "228",
        |	"courseStatus": "PUBLISHED",
        |	"occurredOn": "2021-06-23 08:33:24.921"
        |}
        |]
        |""".stripMargin

    val expectedColumns = Set(
      "course_activity_container_is_accelerated",
      "course_activity_container_pacing",
      "course_activity_container_description",
      "course_activity_container_metadata",
      "course_activity_container_sequence",
      "course_activity_container_course_version",
      "course_activity_container_attach_status",
      "course_activity_container_id",
      "course_activity_container_longname",
      "course_activity_container_updated_time",
      "course_activity_container_dw_created_time",
      "course_activity_container_title",
      "course_activity_container_created_time",
      "course_activity_container_course_id",
      "course_activity_container_domain",
      "course_activity_container_status",
      "course_activity_container_dw_updated_time",
      "course_activity_container_index",
      "rel_course_activity_container_dw_id"
    )

    val sprk = spark
    import sprk.implicits._
    val transformer = new ContainerMutatedTransform(sprk, service)


    val publishedInput = spark.read.json(Seq(publishedValue).toDS()).transform(addMetadata(metadataPublishedValue))
    when(service.readOptional(ContainerPublishedParquetSource, sprk)).thenReturn(Some(publishedInput))

    val addedInput = spark.read.json(Seq(addedValue).toDS()).transform(addMetadata(metadataAddValue))
    when(service.readOptional(ContainerAddedParquetSource, sprk)).thenReturn(Some(addedInput))

    val updatedInput = spark.read.json(Seq(updatedValue).toDS()).transform(addMetadata(metadataUpdatedValue))
    when(service.readOptional(ContainerUpdatedParquetSource, sprk)).thenReturn(Some(updatedInput))

    val deletedInput = spark.read.json(Seq(deletedValue).toDS())
    when(service.readOptional(ContainerDeletedParquetSource, sprk)).thenReturn(Some(deletedInput))

    val sinks = transformer.transform()

    val df = sinks.filter(_.get.name == ContainerTransformedSink).head.get.output

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 6)

    val created = df.filter($"course_activity_container_id" === "level1_id")
    assert(created.count() == 2)
    assert[String](created, "course_activity_container_course_id", "pathwayId")
    assert[Int](created, "course_activity_container_index", 0)
    assert[String](created, "course_activity_container_pacing", "SELF_PACED")
    assert[Int](created, "course_activity_container_sequence", 1)
    assert[Int](created, "course_activity_container_status", 1)
    assert[Int](created, "course_activity_container_attach_status", 1)
    assert[String](created, "course_activity_container_longname", "Phantoms long name")
    assert[String](created, "course_activity_container_description", "Level description")
    assert(created.columns.contains("course_activity_container_metadata"))
    assert[String](created, "course_activity_container_course_version", "228")
    assert[String](created, "course_activity_container_created_time", "2021-06-23 05:33:24.921")
    assert[String](created, "course_activity_container_updated_time", null)
    assert[Boolean](created, "course_activity_container_is_accelerated", false)

    val added = df.filter(($"course_activity_container_id" === "level2_id") && ($"course_activity_container_title" === "Level-2"))
    assert(added.count() == 2)
    assert[String](added, "course_activity_container_course_id", "pathwayId")
    assert[Int](added, "course_activity_container_index", 0)
    assert[String](added, "course_activity_container_pacing", "SELF_PACED")
    assert[Int](added, "course_activity_container_sequence", 1)
    assert[Int](added, "course_activity_container_status", 4)
    assert[Int](added, "course_activity_container_attach_status", 1)
    assert[String](added, "course_activity_container_longname", "Phantoms long name")
    assert[String](added, "course_activity_container_description", "Level description")
    assert(added.columns.contains("course_activity_container_metadata"))
    assert[String](added, "course_activity_container_course_version", "228")
    assert[String](added, "course_activity_container_created_time", "2021-06-23 06:33:24.921")
    assert[String](added, "course_activity_container_updated_time", "2021-06-23 08:33:24.921")

    val updated = df.filter(($"course_activity_container_id" === "level2_id") && ($"course_activity_container_title" === "Level-22"))
    assert(updated.count() == 1)
    assert[String](updated, "course_activity_container_course_id", "pathwayId")
    assert[Int](updated, "course_activity_container_index", 0)
    assert[String](updated, "course_activity_container_pacing", "SELF_PACED")
    assert[Int](updated, "course_activity_container_sequence", 1)
    assert[Int](updated, "course_activity_container_status", 4)
    assert[Int](updated, "course_activity_container_attach_status", 1)
    assert[String](updated, "course_activity_container_longname", "Phantoms long name")
    assert[String](updated, "course_activity_container_description", "Level description")
    assert(updated.columns.contains("course_activity_container_metadata"))
    assert[String](updated, "course_activity_container_course_version", "228")
    assert[String](updated, "course_activity_container_created_time", "2021-06-23 07:33:24.921")
    assert[String](updated, "course_activity_container_updated_time", "2021-06-23 08:33:24.921")

    val deleted = df.filter(($"course_activity_container_id" === "level2_id") && ($"course_activity_container_status" === 1))
    assert(deleted.count() == 1)
    assert[String](deleted, "course_activity_container_course_id", "pathwayId")
    assert[Int](deleted, "course_activity_container_index", 0)
    assert[String](deleted, "course_activity_container_pacing", null)
    assert[Integer](deleted, "course_activity_container_sequence", null)
    assert[Int](deleted, "course_activity_container_status", 1)
    assert[Int](deleted, "course_activity_container_attach_status", 4)
    assert[String](deleted, "course_activity_container_longname", null)
    assert[String](deleted, "course_activity_container_description", null)
    assert[String](deleted, "course_activity_container_metadata", null)
    assert[String](deleted, "course_activity_container_course_version", "228")
    assert[String](deleted, "course_activity_container_created_time", "2021-06-23 08:33:24.921")
    assert[String](deleted, "course_activity_container_updated_time", null)

  }
}
