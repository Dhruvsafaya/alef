package com.alefeducation.dimensions.course_activity_association.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.course_activity_container.transform.ContainerMutatedTransform
import com.alefeducation.dimensions.course_activity_association.transform.CourseActivityAssociationTransform.{CourseActivityAssociationEntity, CourseActivityAssociationTransformService}
import com.alefeducation.util.DataFrameEqualityUtils.{assertSmallDatasetEquality, createDfFromJson}
import com.alefeducation.util.Resources.getSink
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, from_json, lit}
import org.apache.spark.sql.types.TimestampType
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

private object IncomingMessages {
  object Activity {
    val ActivityPlannedMsg: String =
      """
        |[
        |{
        |   "eventType":"ActivityPlannedInCourseEvent",
        |   "loadtime":"2023-01-23T06:32:09.431Z",
        |   "activityId":{
        |      "uuid":"activity_id1",
        |      "id":28379
        |   },
        |   "courseId":"course_id1",
        |   "courseType":"PATHWAY",
        |   "parentItemId":"container_id1",
        |   "index":0,
        |   "courseVersion":"1.0",
        |   "settings":{
        |      "pacing":"UN_LOCKED",
        |      "hideWhenPublishing":false,
        |      "isOptional":false
        |   },
        |   "courseStatus":"PUBLISHED",
        |   "parentItemType":"LEVEL",
        |   "occurredOn":"2023-01-23 06:32:09.319",
        |   "eventDateDw":"20230123",
        |   "isJointParentActivity": false
        |}
        |]
        |""".stripMargin

    val ActivityUnplannedMsg: String =
      """
        |[
        |{
        |   "eventType":"ActivityUnPlannedInCourseEvent",
        |   "loadtime":"2023-01-23T06:32:10.431Z",
        |   "activityId":{
        |      "uuid":"activity_id1",
        |      "id":28379
        |   },
        |   "courseId":"course_id1",
        |   "courseType":"PATHWAY",
        |   "parentItemId":"container_id2",
        |   "index":0,
        |   "courseVersion":"2.0",
        |   "courseStatus":"PUBLISHED",
        |   "parentItemType":"LEVEL",
        |   "occurredOn":"2023-01-23 06:33:09.319",
        |   "eventDateDw":"20230123",
        |   "isJointParentActivity": false
        |}
        |]
        |""".stripMargin

    val ActivityUpdatedMsg: String =
      """
        |[
        |{
        |   "eventType":"ActivityUpdatedInCourseEvent",
        |   "loadtime":"2023-01-23T06:32:13.431Z",
        |   "activityId":{
        |      "uuid":"activity_id1",
        |      "id":28379
        |   },
        |   "courseId":"course_id1",
        |   "courseType":"PATHWAY",
        |   "parentItemId":"container_id3",
        |   "index":0,
        |   "courseVersion":"3.0",
        |   "settings":{
        |      "pacing":"UN_LOCKED",
        |      "hideWhenPublishing":false,
        |      "isOptional":false
        |   },
        |   "courseStatus":"PUBLISHED",
        |   "parentItemType":"LEVEL",
        |   "occurredOn":"2023-01-23 06:34:09.319",
        |   "eventDateDw":"20230123",
        |   "isJointParentActivity": true
        |}
        |]
        |""".stripMargin

    val fullMetadata: String =
      """
        |{
        |      "tags": [
        |        {
        |          "key": "Grade",
        |          "values": [
        |            "1",
        |            "2"
        |          ],
        |          "type": "LIST"
        |        },
        |        {
        |          "key": "Domain",
        |          "values": [
        |            "{\"name\": \"Geometry\",\"icon\": \"Geometry\",\"color\": \"lightBlue\"}",
        |            "{\"name\": \"Algebra and Algebraic Thinking\",\"icon\": \"AlgebraAndAlgebraicThinking\",\"color\": \"lightGreen\"}"
        |          ],
        |          "attributes": [
        |            {
        |              "value": "val01",
        |              "color": "green",
        |              "translation": "tr01"
        |            }
        |          ],
        |          "type": "DOMAIN"
        |         }
        |       ]
        |  }
        |""".stripMargin

    val emptyMetadata: String =
      """
        |{
        |  "tags": []
        |}
        |""".stripMargin
  }

  object InterimCheckpoint {
    val InterimCheckpointCreatedMsg: String =
      """
        |{
        |   "eventType":"InterimCheckpointPublishedEvent",
        |	"id": "activity_id_ip1",
        |	"courseId": "course_id1",
        |   "parentItemId": "container_id_ip1",
        |	"title": "Title1",
        |	"language": "EN_US",
        |	"userTenant": "shared",
        |	"lessonsWithPools": [
        |		{
        |			"id": "0b8e9bf4-b376-4eb5-b3b9-000000028490",
        |			"poolIds": [
        |				"5ea7d1a99c263b0001fd7553",
        |				"6057c3a28d11fb00010d3600"
        |			]
        |		}
        |	],
        |	"createdOn": "2023-01-12T07:38:47.668",
        |	"updatedOn": "2023-01-12T07:38:47.668",
        |	"settings": {
        |		"pacing": "UN_LOCKED",
        |		"hideWhenPublishing": false,
        |		"isOptional": false
        |	},
        |	"index": 12,
        |	"courseVersion": "1.0",
        |	"courseType": "COURSE",
        |	"parentItemType": "CONTAINER",
        |	"courseStatus": "IN_REVIEW",
        |   "isJointParentActivity": true,
        |	"occurredOn": "2023-01-10 06:32:09.319"
        |}
        |""".stripMargin

    val InterimCheckpointUpdatedMsg: String =
      """
        |{
        |   "eventType":"InterimCheckpointUpdatedEvent",
        |	"id": "activity_id_ip1",
        |	"courseId": "course_id1",
        |   "parentItemId": "container_id_ip1",
        |	"title": "Title1",
        |	"language": "EN_US",
        |	"userTenant": "shared",
        |	"lessonsWithPools": [
        |		{
        |			"id": "0b8e9bf4-b376-4eb5-b3b9-000000028490",
        |			"poolIds": [
        |				"5ea7d1a99c263b0001fd7553",
        |				"6057c3a28d11fb00010d3600"
        |			]
        |		}
        |	],
        |	"createdOn": "2023-01-12T07:38:47.668",
        |	"updatedOn": "2023-01-12T07:38:47.668",
        |	"settings": {
        |		"pacing": "UN_LOCKED",
        |		"hideWhenPublishing": false,
        |		"isOptional": false
        |	},
        |	"index": 12,
        |	"courseVersion": "2.0",
        |	"courseType": "COURSE",
        |	"parentItemType": "CONTAINER",
        |	"courseStatus": "PUBLISHED",
        |	"occurredOn":"2023-01-10 07:32:09.319"
        |}
        |""".stripMargin

    val InterimCheckpointDeletedMsg: String =
      """
        |{
        |   "eventType":"InterimCheckpointDeletedEvent",
        |	"id": "activity_id_ip1",
        |	"userTenant": "shared",
        |	"courseId": "course_id1",
        |	"parentItemId": "container_id_ip1",
        |	"index": 9,
        |	"courseVersion": "3.0",
        |	"courseStatus": "PUBLISHED",
        |	"parentItemType": "CONTAINER",
        |	"isParentDeleted": false,
        |	"occurredOn": "2023-01-10 08:36:09.319"
        |}
        |""".stripMargin
  }

}

private object Sources {
  val ActivityPlannedSource = "parquet-published-activity-planned-in-courses-source"
  val ActivityUnplannedSource = "parquet-published-activity-unplanned-in-courses-source"
  val ActivityUpdatedSource = "parquet-published-activity-updated-in-courses-source"
  val InterimCheckpointCreatedSource = "parquet-published-interim-checkpoint-in-courses-created-source"
  val InterimCheckpointUpdatedSource = "parquet-published-interim-checkpoint-in-courses-updated-source"
  val InterimCheckpointDeletedSource = "parquet-published-interim-checkpoint-in-courses-deleted-source"
}
class CourseActivityAssociationTransformSpec extends SparkSuite {

  val service: SparkBatchService = mock[SparkBatchService]

  private val ExpectedColumns: Set[String] = Set(
    "caa_dw_id",
    "caa_container_id",
    "caa_course_id",
    "caa_activity_type",
    "caa_created_time",
    "caa_dw_updated_time",
    "caa_activity_id",
    "caa_status",
    "caa_updated_time",
    "caa_dw_created_time",
    "caa_activity_pacing",
    "caa_attach_status",
    "caa_course_version",
    "caa_activity_index",
    "caa_is_parent_deleted",
    "caa_is_joint_parent_activity",
    "caa_activity_is_optional",
    "caa_metadata",
    "caa_grade",
  )

  private def createDfFromJsonWithTimeCols(prefix: String, json: String): DataFrame = {
    val df = createDfFromJson(spark, json)
      .withColumn(s"${prefix}_created_time", col(s"${prefix}_created_time").cast(TimestampType))

    if (df.columns.contains(s"${prefix}_updated_time")) {
      df.withColumn(s"${prefix}_updated_time", col(s"${prefix}_updated_time").cast(TimestampType))
    } else df
  }

  private def addMetadata(jsonValue: String)(df: DataFrame): DataFrame =
    df.withColumn("metadata", from_json(lit(jsonValue), ContainerMutatedTransform.metadataSchema))

  test("should construct activity and interim checkpoint sink with transformed fields") {
    val expectedActivityPlannedJson =
      """
        |[
        | {"caa_dw_id":4, "caa_course_id":"course_id1", "caa_activity_type":1, "caa_attach_status":1, "caa_activity_pacing":"UN_LOCKED", "caa_activity_id":"activity_id1", "caa_status":2, "caa_container_id":"container_id1", "caa_created_time":"2023-01-23 06:32:09.319", "caa_dw_created_time":"2023-02-03 10:06:07.655", "caa_dw_updated_time":null, "caa_is_parent_deleted":false, "caa_course_version":"1.0", "caa_activity_index":0, "caa_is_joint_parent_activity":false, "caa_grade":"1", "caa_updated_time": "2023-01-23 06:34:09.319", "caa_activity_is_optional": false},
        | {"caa_dw_id":5, "caa_course_id":"course_id1", "caa_activity_type":1, "caa_attach_status":1, "caa_activity_pacing":"UN_LOCKED", "caa_activity_id":"activity_id1", "caa_status":2, "caa_container_id":"container_id1", "caa_created_time":"2023-01-23 06:32:09.319", "caa_dw_created_time":"2023-02-03 10:06:07.655", "caa_dw_updated_time":null, "caa_is_parent_deleted":false, "caa_course_version":"1.0", "caa_activity_index":0, "caa_is_joint_parent_activity":false, "caa_grade":"2", "caa_updated_time": "2023-01-23 06:34:09.319", "caa_activity_is_optional": false}
        |]
        |""".stripMargin

    val expectedActivityUnplannedJson =
      """
        |[
        |{"caa_dw_id":6, "caa_course_id":"course_id1", "caa_activity_type":1, "caa_attach_status":4, "caa_activity_pacing": null, "caa_activity_id":"activity_id1", "caa_status":2, "caa_updated_time":"2023-01-23 06:34:09.319", "caa_container_id":"container_id2", "caa_created_time":"2023-01-23 06:33:09.319", "caa_dw_created_time":"2023-02-03 10:06:07.655", "caa_dw_updated_time":null, "caa_is_parent_deleted":false, "caa_course_version":"2.0", "caa_activity_index":0, "caa_is_joint_parent_activity":false, "caa_grade":null, "caa_activity_is_optional": null}
        |]
        |""".stripMargin

    val expectedActivityUpdatedJson =
      """
        |[
        |{"caa_dw_id":7, "caa_course_id":"course_id1", "caa_activity_type":1, "caa_attach_status":1, "caa_activity_pacing":"UN_LOCKED", "caa_activity_id":"activity_id1", "caa_status":1, "caa_updated_time":null, "caa_container_id":"container_id3", "caa_created_time":"2023-01-23 06:34:09.319", "caa_dw_created_time":"2023-02-03 10:06:07.655", "caa_dw_updated_time":null, "caa_is_parent_deleted":false, "caa_course_version":"3.0", "caa_activity_index":0, "caa_is_joint_parent_activity":true, "caa_grade":"1", "caa_activity_is_optional": false},
        |{"caa_dw_id":8, "caa_course_id":"course_id1", "caa_activity_type":1, "caa_attach_status":1, "caa_activity_pacing":"UN_LOCKED", "caa_activity_id":"activity_id1", "caa_status":1, "caa_updated_time":null, "caa_container_id":"container_id3", "caa_created_time":"2023-01-23 06:34:09.319", "caa_dw_created_time":"2023-02-03 10:06:07.655", "caa_dw_updated_time":null, "caa_is_parent_deleted":false, "caa_course_version":"3.0", "caa_activity_index":0, "caa_is_joint_parent_activity":true, "caa_grade":"2", "caa_activity_is_optional": false}
        |]
        |""".stripMargin

    val expectedInterimCheckpointJson =
      """
        |[
        |{"caa_dw_id":1, "caa_created_time":"2023-01-10 06:32:09.319", "caa_updated_time":"2023-01-10 08:36:09.319", "caa_dw_created_time":"2023-02-03 10:06:07.655", "caa_dw_updated_time":null, "caa_status":2, "caa_attach_status":1, "caa_course_id":"course_id1", "caa_container_id":"container_id_ip1", "caa_activity_id":"activity_id_ip1", "caa_activity_type":2, "caa_activity_pacing":"UN_LOCKED", "caa_is_parent_deleted":false, "caa_course_version":"1.0", "caa_activity_index":12, "caa_is_joint_parent_activity":true, "caa_grade":null, "caa_activity_is_optional":false, "caa_metadata": null},
        |{"caa_dw_id":2, "caa_created_time":"2023-01-10 07:32:09.319", "caa_updated_time":"2023-01-10 08:36:09.319", "caa_dw_created_time":"2023-02-03 10:06:07.655", "caa_dw_updated_time":null, "caa_status":2, "caa_attach_status":1, "caa_course_id":"course_id1", "caa_container_id":"container_id_ip1", "caa_activity_id":"activity_id_ip1", "caa_activity_type":2, "caa_activity_pacing":"UN_LOCKED", "caa_is_parent_deleted":false, "caa_course_version":"2.0", "caa_activity_index":12, "caa_is_joint_parent_activity":null, "caa_grade":null, "caa_activity_is_optional":false, "caa_metadata": null},
        |{"caa_dw_id":3, "caa_created_time":"2023-01-10 08:36:09.319", "caa_updated_time":null, "caa_dw_created_time":"2023-02-03 10:06:07.655", "caa_dw_updated_time":null, "caa_status":1, "caa_attach_status":4, "caa_course_id":"course_id1", "caa_container_id":"container_id_ip1", "caa_activity_id":"activity_id_ip1", "caa_activity_type":2, "caa_activity_pacing":null, "caa_is_parent_deleted":false, "caa_course_version":"3.0", "caa_activity_index":9, "caa_is_joint_parent_activity":null, "caa_grade":null, "caa_activity_is_optional":null, "caa_metadata": null}
        |]
        |""".stripMargin

    val sinkName = getSink(CourseActivityAssociationTransformService).head

    // Mocks
    val sprk = spark
    import sprk.implicits._

    when(service.getStartIdUpdateStatus("dim_course_activity_association")).thenReturn(1)

    val activityPlanned =
      spark.read
        .json(Seq(IncomingMessages.Activity.ActivityPlannedMsg).toDS())
        .transform(addMetadata(IncomingMessages.Activity.fullMetadata))
    when(service.readOptional(Sources.ActivityPlannedSource, sprk)).thenReturn(Some(activityPlanned))

    val activityUnplanned =
      spark.read
        .json(Seq(IncomingMessages.Activity.ActivityUnplannedMsg).toDS())
        .transform(addMetadata(IncomingMessages.Activity.emptyMetadata))
    when(service.readOptional(Sources.ActivityUnplannedSource, sprk)).thenReturn(Some(activityUnplanned))

    val activityUpdated =
      spark.read
        .json(Seq(IncomingMessages.Activity.ActivityUpdatedMsg).toDS())
        .transform(addMetadata(IncomingMessages.Activity.fullMetadata))
    when(service.readOptional(Sources.ActivityUpdatedSource, sprk)).thenReturn(Some(activityUpdated))

    val interimCheckpointCreated = spark.read.json(Seq(IncomingMessages.InterimCheckpoint.InterimCheckpointCreatedMsg).toDS())
    when(
      service.readUniqueOptional(
        Sources.InterimCheckpointCreatedSource, sprk, uniqueColNames = List("courseId", "id", "courseVersion")
      )
    ).thenReturn(Some(interimCheckpointCreated))

    val interimCheckpointUpdated = spark.read.json(Seq(IncomingMessages.InterimCheckpoint.InterimCheckpointUpdatedMsg).toDS())
    when(
      service.readUniqueOptional(
        Sources.InterimCheckpointUpdatedSource, sprk, uniqueColNames = List("courseId", "id", "courseVersion")
      )
    ).thenReturn(Some(interimCheckpointUpdated))

    val interimCheckpointDeleted = spark.read.json(Seq(IncomingMessages.InterimCheckpoint.InterimCheckpointDeletedMsg).toDS())
    when(
      service.readUniqueOptional(
        Sources.InterimCheckpointDeletedSource, sprk, uniqueColNames = List("courseId", "id", "courseVersion")
      )
    ).thenReturn(Some(interimCheckpointDeleted))

    // Transforms
    val transformer = new CourseActivityAssociationTransform(spark, service)
    val sink = transformer.transform(
      activitySources = List(
        Sources.ActivityPlannedSource,
        Sources.ActivityUnplannedSource,
        Sources.ActivityUpdatedSource
      ),
      interimCheckpointSources = List(
        Sources.InterimCheckpointCreatedSource,
        Sources.InterimCheckpointUpdatedSource,
        Sources.InterimCheckpointDeletedSource
      ),
      sinkName = sinkName
    )
    val df = sink.filter(_.name === sinkName).head.output

    // Asserts
    assert(df.columns.toSet === ExpectedColumns)

    val expectedPlannedDf = createDfFromJsonWithTimeCols(CourseActivityAssociationEntity, expectedActivityPlannedJson)
      .transform(addMetadata(IncomingMessages.Activity.fullMetadata))
      .withColumnRenamed("metadata", "caa_metadata")
    val actualPlannedDf = df.filter(col("caa_container_id") === "container_id1")
    assertSmallDatasetEquality(CourseActivityAssociationEntity, actualPlannedDf, expectedPlannedDf)

    val expectedUnPlannedDf = createDfFromJsonWithTimeCols(CourseActivityAssociationEntity, expectedActivityUnplannedJson)
      .transform(addMetadata(IncomingMessages.Activity.emptyMetadata))
      .withColumnRenamed("metadata", "caa_metadata")
    val actualUnPlannedDf = df.filter(col("caa_container_id") === "container_id2")
    assertSmallDatasetEquality(CourseActivityAssociationEntity, actualUnPlannedDf, expectedUnPlannedDf)

    val expectedUpdatedDf = createDfFromJsonWithTimeCols(CourseActivityAssociationEntity, expectedActivityUpdatedJson)
      .transform(addMetadata(IncomingMessages.Activity.fullMetadata))
      .withColumnRenamed("metadata", "caa_metadata")
    val actualUpdatedDf = df.filter(col("caa_container_id") === "container_id3")
    assertSmallDatasetEquality(CourseActivityAssociationEntity, actualUpdatedDf, expectedUpdatedDf)

    val expectedInterimCheckpoint = createDfFromJsonWithTimeCols(CourseActivityAssociationEntity, expectedInterimCheckpointJson)
    val actualInterimCheckpoint = df.filter(col("caa_activity_type") === 2)
    assertSmallDatasetEquality(CourseActivityAssociationEntity, actualInterimCheckpoint, expectedInterimCheckpoint)
  }

  test("should construct activity sink with transformed fields") {
    val expectedActivityPlannedJson =
      """
        |[
        | {"caa_dw_id":1, "caa_course_id":"course_id1", "caa_activity_type":1, "caa_attach_status":1, "caa_activity_pacing":"UN_LOCKED", "caa_activity_id":"activity_id1", "caa_status":2, "caa_container_id":"container_id1", "caa_created_time":"2023-01-23 06:32:09.319", "caa_dw_created_time":"2023-02-03 10:06:07.655", "caa_dw_updated_time":null, "caa_is_parent_deleted":false, "caa_course_version":"1.0", "caa_activity_index":0, "caa_is_joint_parent_activity":false, "caa_grade":"1", "caa_updated_time": "2023-01-23 06:34:09.319", "caa_activity_is_optional": false},
        | {"caa_dw_id":2, "caa_course_id":"course_id1", "caa_activity_type":1, "caa_attach_status":1, "caa_activity_pacing":"UN_LOCKED", "caa_activity_id":"activity_id1", "caa_status":2, "caa_container_id":"container_id1", "caa_created_time":"2023-01-23 06:32:09.319", "caa_dw_created_time":"2023-02-03 10:06:07.655", "caa_dw_updated_time":null, "caa_is_parent_deleted":false, "caa_course_version":"1.0", "caa_activity_index":0, "caa_is_joint_parent_activity":false, "caa_grade":"2", "caa_updated_time": "2023-01-23 06:34:09.319", "caa_activity_is_optional": false}
        |]
        |""".stripMargin

    val expectedActivityUnplannedJson =
      """
        |[
        |{"caa_dw_id":3, "caa_course_id":"course_id1", "caa_activity_type":1, "caa_attach_status":4, "caa_activity_pacing": null, "caa_activity_id":"activity_id1", "caa_status":2, "caa_updated_time":"2023-01-23 06:34:09.319", "caa_container_id":"container_id2", "caa_created_time":"2023-01-23 06:33:09.319", "caa_dw_created_time":"2023-02-03 10:06:07.655", "caa_dw_updated_time":null, "caa_is_parent_deleted":false, "caa_course_version":"2.0", "caa_activity_index":0, "caa_is_joint_parent_activity":false, "caa_grade":null, "caa_activity_is_optional": null}
        |]
        |""".stripMargin

    val expectedActivityUpdatedJson =
      """
        |[
        |{"caa_dw_id":4, "caa_course_id":"course_id1", "caa_activity_type":1, "caa_attach_status":1, "caa_activity_pacing":"UN_LOCKED", "caa_activity_id":"activity_id1", "caa_status":1, "caa_updated_time":null, "caa_container_id":"container_id3", "caa_created_time":"2023-01-23 06:34:09.319", "caa_dw_created_time":"2023-02-03 10:06:07.655", "caa_dw_updated_time":null, "caa_is_parent_deleted":false, "caa_course_version":"3.0", "caa_activity_index":0, "caa_is_joint_parent_activity":true, "caa_grade":"1", "caa_activity_is_optional": false},
        |{"caa_dw_id":5, "caa_course_id":"course_id1", "caa_activity_type":1, "caa_attach_status":1, "caa_activity_pacing":"UN_LOCKED", "caa_activity_id":"activity_id1", "caa_status":1, "caa_updated_time":null, "caa_container_id":"container_id3", "caa_created_time":"2023-01-23 06:34:09.319", "caa_dw_created_time":"2023-02-03 10:06:07.655", "caa_dw_updated_time":null, "caa_is_parent_deleted":false, "caa_course_version":"3.0", "caa_activity_index":0, "caa_is_joint_parent_activity":true, "caa_grade":"2", "caa_activity_is_optional": false}
        |]
        |""".stripMargin

    val sinkName = getSink(CourseActivityAssociationTransformService).head

    // Mocks
    val sprk = spark
    import sprk.implicits._

    when(service.getStartIdUpdateStatus("dim_course_activity_association")).thenReturn(1)

    val activityPlanned =
      spark.read
        .json(Seq(IncomingMessages.Activity.ActivityPlannedMsg).toDS())
        .transform(addMetadata(IncomingMessages.Activity.fullMetadata))
    when(service.readOptional(Sources.ActivityPlannedSource, sprk)).thenReturn(Some(activityPlanned))

    val activityUnplanned =
      spark.read
        .json(Seq(IncomingMessages.Activity.ActivityUnplannedMsg).toDS())
        .transform(addMetadata(IncomingMessages.Activity.emptyMetadata))
    when(service.readOptional(Sources.ActivityUnplannedSource, sprk)).thenReturn(Some(activityUnplanned))

    val activityUpdated =
      spark.read
        .json(Seq(IncomingMessages.Activity.ActivityUpdatedMsg).toDS())
        .transform(addMetadata(IncomingMessages.Activity.fullMetadata))
    when(service.readOptional(Sources.ActivityUpdatedSource, sprk)).thenReturn(Some(activityUpdated))

    when(
      service.readUniqueOptional(
        Sources.InterimCheckpointCreatedSource, sprk, uniqueColNames = List("courseId", "id", "courseVersion")
      )
    ).thenReturn(None)
    when(
      service.readUniqueOptional(
        Sources.InterimCheckpointUpdatedSource, sprk, uniqueColNames = List("courseId", "id", "courseVersion")
      )
    ).thenReturn(None)
    when(
      service.readUniqueOptional(
        Sources.InterimCheckpointDeletedSource, sprk, uniqueColNames = List("courseId", "id", "courseVersion")
      )
    ).thenReturn(None)

    // Transforms
    val transformer = new CourseActivityAssociationTransform(spark, service)
    val sink = transformer.transform(
      activitySources = List(
        Sources.ActivityPlannedSource,
        Sources.ActivityUnplannedSource,
        Sources.ActivityUpdatedSource
      ),
      interimCheckpointSources = List(
        Sources.InterimCheckpointCreatedSource,
        Sources.InterimCheckpointUpdatedSource,
        Sources.InterimCheckpointDeletedSource
      ),
      sinkName = sinkName
    )
    val df = sink.filter(_.name === sinkName).head.output

    // Asserts
    assert(df.columns.toSet === ExpectedColumns)
    assert(df.count() === 5)

    val expectedPlannedDf = createDfFromJsonWithTimeCols(CourseActivityAssociationEntity, expectedActivityPlannedJson)
      .transform(addMetadata(IncomingMessages.Activity.fullMetadata))
      .withColumnRenamed("metadata", "caa_metadata")
    val actualPlannedDf = df.filter(col("caa_container_id") === "container_id1")
    assertSmallDatasetEquality(CourseActivityAssociationEntity, actualPlannedDf, expectedPlannedDf)

    val expectedUnPlannedDf = createDfFromJsonWithTimeCols(CourseActivityAssociationEntity, expectedActivityUnplannedJson)
      .transform(addMetadata(IncomingMessages.Activity.emptyMetadata))
      .withColumnRenamed("metadata", "caa_metadata")
    val actualUnPlannedDf = df.filter(col("caa_container_id") === "container_id2")
    assertSmallDatasetEquality(CourseActivityAssociationEntity, actualUnPlannedDf, expectedUnPlannedDf)

    val expectedUpdatedDf = createDfFromJsonWithTimeCols(CourseActivityAssociationEntity, expectedActivityUpdatedJson)
      .transform(addMetadata(IncomingMessages.Activity.fullMetadata))
      .withColumnRenamed("metadata", "caa_metadata")
    val actualUpdatedDf = df.filter(col("caa_container_id") === "container_id3")
    assertSmallDatasetEquality(CourseActivityAssociationEntity, actualUpdatedDf, expectedUpdatedDf)
  }

  test("should construct interim checkpoint sink with transformed fields") {
    val expectedInterimCheckpointJson =
      """
        |[
        |{"caa_dw_id":1, "caa_created_time":"2023-01-10 06:32:09.319", "caa_updated_time":"2023-01-10 08:36:09.319", "caa_dw_created_time":"2023-02-03 10:06:07.655", "caa_dw_updated_time":null, "caa_status":2, "caa_attach_status":1, "caa_course_id":"course_id1", "caa_container_id":"container_id_ip1", "caa_activity_id":"activity_id_ip1", "caa_activity_type":2, "caa_activity_pacing":"UN_LOCKED", "caa_is_parent_deleted":false, "caa_course_version":"1.0", "caa_activity_index":12, "caa_is_joint_parent_activity":true, "caa_grade":null, "caa_activity_is_optional":false, "caa_metadata": null},
        |{"caa_dw_id":2, "caa_created_time":"2023-01-10 07:32:09.319", "caa_updated_time":"2023-01-10 08:36:09.319", "caa_dw_created_time":"2023-02-03 10:06:07.655", "caa_dw_updated_time":null, "caa_status":2, "caa_attach_status":1, "caa_course_id":"course_id1", "caa_container_id":"container_id_ip1", "caa_activity_id":"activity_id_ip1", "caa_activity_type":2, "caa_activity_pacing":"UN_LOCKED", "caa_is_parent_deleted":false, "caa_course_version":"2.0", "caa_activity_index":12, "caa_is_joint_parent_activity":null, "caa_grade":null, "caa_activity_is_optional":false, "caa_metadata": null},
        |{"caa_dw_id":3, "caa_created_time":"2023-01-10 08:36:09.319", "caa_updated_time":null, "caa_dw_created_time":"2023-02-03 10:06:07.655", "caa_dw_updated_time":null, "caa_status":1, "caa_attach_status":4, "caa_course_id":"course_id1", "caa_container_id":"container_id_ip1", "caa_activity_id":"activity_id_ip1", "caa_activity_type":2, "caa_activity_pacing":null, "caa_is_parent_deleted":false, "caa_course_version":"3.0", "caa_activity_index":9, "caa_is_joint_parent_activity":null, "caa_grade":null, "caa_activity_is_optional":null, "caa_metadata": null}
        |]
        |""".stripMargin

    val sinkName = getSink(CourseActivityAssociationTransformService).head

    // Mocks
    val sprk = spark
    import sprk.implicits._

    when(service.getStartIdUpdateStatus("dim_course_activity_association")).thenReturn(1)

    when(service.readOptional(Sources.ActivityPlannedSource, sprk)).thenReturn(None)
    when(service.readOptional(Sources.ActivityUnplannedSource, sprk)).thenReturn(None)
    when(service.readOptional(Sources.ActivityUpdatedSource, sprk)).thenReturn(None)

    val interimCheckpointCreated = spark.read.json(Seq(IncomingMessages.InterimCheckpoint.InterimCheckpointCreatedMsg).toDS())
    when(
      service.readUniqueOptional(
        Sources.InterimCheckpointCreatedSource, sprk, uniqueColNames = List("courseId", "id", "courseVersion")
      )
    ).thenReturn(Some(interimCheckpointCreated))

    val interimCheckpointUpdated = spark.read.json(Seq(IncomingMessages.InterimCheckpoint.InterimCheckpointUpdatedMsg).toDS())
    when(
      service.readUniqueOptional(
        Sources.InterimCheckpointUpdatedSource, sprk, uniqueColNames = List("courseId", "id", "courseVersion")
      )
    ).thenReturn(Some(interimCheckpointUpdated))

    val interimCheckpointDeleted = spark.read.json(Seq(IncomingMessages.InterimCheckpoint.InterimCheckpointDeletedMsg).toDS())
    when(
      service.readUniqueOptional(
        Sources.InterimCheckpointDeletedSource, sprk, uniqueColNames = List("courseId", "id", "courseVersion")
      )
    ).thenReturn(Some(interimCheckpointDeleted))

    // Transforms
    val transformer = new CourseActivityAssociationTransform(spark, service)
    val sink = transformer.transform(
      activitySources = List(
        Sources.ActivityPlannedSource,
        Sources.ActivityUnplannedSource,
        Sources.ActivityUpdatedSource
      ),
      interimCheckpointSources = List(
        Sources.InterimCheckpointCreatedSource,
        Sources.InterimCheckpointUpdatedSource,
        Sources.InterimCheckpointDeletedSource
      ),
      sinkName = sinkName
    )
    val df = sink.filter(_.name === sinkName).head.output

    // Asserts
    assert(df.columns.toSet === ExpectedColumns)
    assert(df.count() === 3)

    val expectedInterimCheckpoint = createDfFromJsonWithTimeCols(CourseActivityAssociationEntity, expectedInterimCheckpointJson)
    val actualInterimCheckpoint = df.filter(col("caa_activity_type") === 2)
    assertSmallDatasetEquality(CourseActivityAssociationEntity, actualInterimCheckpoint, expectedInterimCheckpoint)
  }

}
