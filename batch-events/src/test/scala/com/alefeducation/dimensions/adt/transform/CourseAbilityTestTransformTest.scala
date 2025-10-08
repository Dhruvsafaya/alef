package com.alefeducation.dimensions.adt.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{from_json, lit}
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class CourseAbilityTestTransformTest extends SparkSuite {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  val expectedColumns: Set[String] = Set(
    "cata_dw_id",
    "cata_course_id",
    "cata_ability_test_activity_uuid",
    "cata_ability_test_activity_id",
    "cata_ability_test_id",
    "cata_ability_test_type",
    "cata_max_attempts",
    "cata_ability_test_pacing",
    "cata_ability_test_index",
    "cata_course_version",
    "cata_is_placement_test",
    "cata_status",
    "cata_attach_status",
    "cata_ability_test_activity_type",
    "cata_metadata",
    "cata_created_time",
    "cata_dw_created_time",
    "cata_dw_updated_time",
    "cata_updated_time"
  )

  val fullMetadata: String =
    """
      |{
      |  "tags": [
      |    {
      |      "key": "Grade",
      |      "values": ["1", "2"],
      |      "type": "LIST"
      |    },
      |    {
      |      "key": "Domain",
      |      "values": [
      |        "{\"name\": \"Geometry\",\"icon\": \"Geometry\",\"color\": \"lightBlue\"}",
      |        "{\"name\": \"Algebra and Algebraic Thinking\",\"icon\": \"AlgebraAndAlgebraicThinking\",\"color\": \"lightGreen\"}"
      |      ],
      |      "attributes": [
      |        {
      |          "value": "val01",
      |          "color": "green",
      |          "translation": "tr01"
      |        }
      |      ],
      |      "type": "DOMAIN"
      |    }
      |  ],
      |  "version": "1"
      |}
      |""".stripMargin

  test("should construct dataframe from TestPlannedInAbilityTestComponentEvent") {
    val plannedValue =
      """
        |[
        |{
        | "eventType": "TestPlannedInAbilityTestComponentEvent",
        | "id": "0e766e6d-db2c-43c4-bb5c-000000028323",
        | "type": "TEST_PART",
        | "maxAttempts": 1,
        | "settings": {
        |   "pacing": "LOCKED",
        |	"hideWhenPublishing": false,
        |   "isOptional": false
        | },
        | "legacyId": 28323,
        | "parentComponentId": "c58be86c-6487-4143-b091-afaf196bd6d1",
        | "index": 0,
        | "courseId": "8fb8cc19-8b09-468a-b556-891f9ac7ee9f",
        | "parentComponentType": "DIAGNOSTIC_TEST",
        | "courseType": "CORE",
        | "isPlacementTest": false,
        | "courseVersion": "1.0",
        | "courseStatus": "PUBLISHED",
        | "occurredOn": "2021-06-23 05:33:24.921"
        |}
        |]
        |""".stripMargin
    val updatedValue =
      """
        |[
        |{
        | "eventType": "TestUpdatedInAbilityTestComponentEvent",
        | "id": "0e766e6d-db2c-43c4-bb5c-000000028323",
        | "type": "TEST_UPDATED",
        | "maxAttempts": 1,
        | "settings": {
        |   "pacing": "SLOW",
        |	"hideWhenPublishing": false,
        |   "isOptional": false
        | },
        | "legacyId": 28323,
        | "parentComponentId": "c58be86c-6487-4143-b091-afaf196bd6d1",
        | "index": 0,
        | "courseId": "8fb8cc19-8b09-468a-b556-891f9ac7ee9f",
        | "parentComponentType": "DIAGNOSTIC_TEST",
        | "courseType": "CORE",
        | "courseVersion": "1.0",
        | "courseStatus": "PUBLISHED",
        | "isPlacementTest": true,
        | "occurredOn": "2021-06-23 07:00:00.000"
        |}
        |]
        |""".stripMargin
    val unPlannedValue =
      """
        |[
        |{
        | "eventType": "TestUnPlannedInAbilityTestComponentEvent",
        | "id": "0e766e6d-db2c-43c4-bb5c-000000028323",
        | "type": "TEST_UNPLANNED",
        | "maxAttempts": 1,
        | "settings": {
        |   "pacing": "SLOW",
        |	"hideWhenPublishing": false,
        |   "isOptional": false
        | },
        | "legacyId": 28323,
        | "parentComponentId": "c58be86c-6487-4143-b091-afaf196bd6d1",
        | "index": 0,
        | "courseId": "8fb8cc19-8b09-468a-b556-891f9ac7ee9f",
        | "parentComponentType": "DIAGNOSTIC_TEST",
        | "courseType": "CORE",
        | "courseVersion": "1.0",
        | "courseStatus": "PUBLISHED",
        | "isPlacementTest": false,
        | "occurredOn": "2021-06-23 10:00:00.000"
        |}
        |]
        |""".stripMargin

    val sprk = spark
    import sprk.implicits._
    val transformer = new CourseAbilityTestTransform(sprk, service, "course-ability-test-transform")

    val plannedDF = spark.read.json(Seq(plannedValue).toDS()).withColumn("metadata", from_json(lit(fullMetadata), CourseAbilityTestTransform.metadataSchema))
    val updatedDF = spark.read.json(Seq(updatedValue).toDS()).withColumn("metadata", from_json(lit(fullMetadata), CourseAbilityTestTransform.metadataSchema))
    val unPlannedDF = spark.read.json(Seq(unPlannedValue).toDS()).withColumn("metadata", from_json(lit(fullMetadata), CourseAbilityTestTransform.metadataSchema))
    when(service.readOptional("parquet-test-planned-in-ability-test-component-source", sprk)).thenReturn(Some(plannedDF))
    when(service.readOptional("parquet-test-updated-in-ability-test-component-source", sprk)).thenReturn(Some(updatedDF))
    when(service.readOptional("parquet-test-unplanned-in-ability-test-component-source", sprk)).thenReturn(Some(unPlannedDF))
    when(service.getStartIdUpdateStatus("dim_course_ability_test_association")).thenReturn(1)

    val sinks = transformer.transform()

    val df = sinks.filter(_.get.name == "parquet-course-ability-test-transformed-sink").head.get.output

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 3)

    val planned = df.filter($"cata_created_time" === "2021-06-23 05:33:24.921")
    assert[String](planned, "cata_ability_test_activity_uuid", "0e766e6d-db2c-43c4-bb5c-000000028323")
    assert[String](planned, "cata_ability_test_activity_type", "TEST_PART")
    assert[Int](planned, "cata_max_attempts", 1)
    assert[String](planned, "cata_ability_test_pacing", "LOCKED")
    assert[Int](planned, "cata_ability_test_activity_id", 28323)
    assert[String](planned, "cata_ability_test_id", "c58be86c-6487-4143-b091-afaf196bd6d1")
    assert[Int](planned, "cata_ability_test_index", 0)
    assert[String](planned, "cata_course_id", "8fb8cc19-8b09-468a-b556-891f9ac7ee9f")
    assert[Int](planned, "cata_ability_test_type", 1)
    assert[String](planned, "cata_course_version", "1.0")
    assert[Int](planned, "cata_status", 4)
    assert[Int](planned, "cata_attach_status", 1)
    assert[String](planned, "cata_updated_time", "2021-06-23 10:00:00.0")
    assert[Boolean](planned, "cata_is_placement_test", false)
    assert[Int](planned, "cata_dw_id", 1)

    val updated = df.filter($"cata_created_time" === "2021-06-23 07:00:00.000")
    assert[String](updated, "cata_ability_test_activity_uuid", "0e766e6d-db2c-43c4-bb5c-000000028323")
    assert[String](updated, "cata_ability_test_activity_type", "TEST_UPDATED")
    assert[Int](updated, "cata_max_attempts", 1)
    assert[String](updated, "cata_ability_test_pacing", "SLOW")
    assert[Int](updated, "cata_ability_test_activity_id", 28323)
    assert[String](updated, "cata_ability_test_id", "c58be86c-6487-4143-b091-afaf196bd6d1")
    assert[Int](updated, "cata_ability_test_index", 0)
    assert[String](updated, "cata_course_id", "8fb8cc19-8b09-468a-b556-891f9ac7ee9f")
    assert[Int](updated, "cata_ability_test_type", 1)
    assert[String](updated, "cata_course_version", "1.0")
    assert[Int](updated, "cata_status", 4)
    assert[Int](updated, "cata_attach_status", 1)
    assert[String](updated, "cata_updated_time", "2021-06-23 10:00:00.0")
    assert[Boolean](updated, "cata_is_placement_test", true)
    assert[Int](updated, "cata_dw_id", 2)


    val unplanned = df.filter($"cata_created_time" === "2021-06-23 10:00:00.000")
    assert[String](unplanned, "cata_ability_test_activity_uuid", "0e766e6d-db2c-43c4-bb5c-000000028323")
    assert[String](unplanned, "cata_ability_test_activity_type", "TEST_UNPLANNED")
    assert[Int](unplanned, "cata_max_attempts", 1)
    assert[String](unplanned, "cata_ability_test_pacing", "SLOW")
    assert[Int](unplanned, "cata_ability_test_activity_id", 28323)
    assert[String](unplanned, "cata_ability_test_id", "c58be86c-6487-4143-b091-afaf196bd6d1")
    assert[Int](unplanned, "cata_ability_test_index", 0)
    assert[String](unplanned, "cata_course_id", "8fb8cc19-8b09-468a-b556-891f9ac7ee9f")
    assert[Int](unplanned, "cata_ability_test_type", 1)
    assert[String](unplanned, "cata_course_version", "1.0")
    assert[Int](unplanned, "cata_status", 1)
    assert[Int](unplanned, "cata_attach_status", 0)
    assert[String](unplanned, "cata_updated_time", null)
    assert[Boolean](unplanned, "cata_is_placement_test", false)
    assert[Int](unplanned, "cata_dw_id", 3)
  }
}
