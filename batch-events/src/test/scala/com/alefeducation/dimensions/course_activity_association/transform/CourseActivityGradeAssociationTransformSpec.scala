package com.alefeducation.dimensions.course_activity_association.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.course_activity_association.transform.CourseActivityGradeAssociationTransform.{ CourseActivityGradeAssociationService}
import com.alefeducation.util.DataFrameEqualityUtils.{assertSmallDatasetEquality, createDfFromJson}
import com.alefeducation.util.Resources.{getSink, getSource}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class CourseActivityGradeAssociationTransformSpec extends SparkSuite {

  private val service: SparkBatchService = mock[SparkBatchService]

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
      |},
      |{
      |   "eventType":"ActivityPlannedInCourseEvent",
      |   "loadtime":"2023-01-23T06:34:09.431Z",
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
      |   "occurredOn":"2023-01-23 06:33:09.319",
      |   "eventDateDw":"20230123",
      |   "isJointParentActivity": false,
      |   "metadata": {
      |    "tags": [
      |      {
      |        "key": "Grade",
      |        "values": [
      |          "7"
      |        ],
      |        "attributes": [
      |          {
      |            "value": "KG",
      |            "color": "lightGray",
      |            "translation": null
      |          }
      |        ],
      |        "type": "LIST"
      |      }
      |    ]
      |  }
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
      |   "isJointParentActivity": true,
      |   "metadata": {
      |    "tags": [
      |      {
      |        "key": "Grade",
      |        "values": [
      |          "8"
      |        ],
      |        "attributes": [
      |          {
      |            "value": "KG",
      |            "color": "lightGray",
      |            "translation": null
      |          }
      |        ],
      |        "type": "LIST"
      |      }
      |    ]
      |  }
      |}
      |]
      |""".stripMargin

  test("should create activity association grade dataframe") {

    val expectedJson: String =
      """
        |[
        |{"caga_dw_id": 1, "caga_course_id":"course_id1","caga_status":1,"caga_activity_id":"activity_id1","caga_created_time":"2023-01-23T06:34:09.319Z","caga_dw_created_time":"2023-07-18T09:41:30.052Z","caga_updated_time":null,"caga_dw_updated_time":null,"caga_grade_id":"8"}
        |]
        |""".stripMargin

    val sinkName = getSink(CourseActivityGradeAssociationService).head

    when(service.getStartIdUpdateStatus("dim_course_activity_grade_association")).thenReturn(1)

    val sprk: SparkSession = spark
    import sprk.implicits._

    val activityPlanned =
      spark.read
        .json(Seq(ActivityPlannedMsg).toDS())


    val activityPlannedSource = "parquet-published-activity-planned-in-courses-source"
    when(service.readOptional(activityPlannedSource, sprk)).thenReturn(Some(activityPlanned))

    val activityUpdated =
      spark.read
        .json(Seq(ActivityUpdatedMsg).toDS())


    val activityUpdatedSource = "parquet-published-activity-updated-in-courses-source"
    when(service.readOptional(activityUpdatedSource, sprk)).thenReturn(Some(activityUpdated))

    val transformer = new CourseActivityGradeAssociationTransform(spark, service)

    val sinks = transformer.transform(CourseActivityGradeAssociationService)
    val df = sinks.filter(_.name === sinkName).head.output

    val expectedCourseActivityGradeAssociationColumns: Set[String] = Set(
      "caga_dw_updated_time",
      "caga_grade_id",
      "caga_created_time",
      "caga_status",
      "caga_updated_time",
      "caga_course_id",
      "caga_activity_id",
      "caga_dw_created_time",
      "caga_dw_id"
    )
    assert(df.columns.toSet === expectedCourseActivityGradeAssociationColumns)
    val expDf = createDfFromJsonWithTimeCols("caga", expectedJson)
    assertSmallDatasetEquality("caga", df, expDf)
  }

  private def createDfFromJsonWithTimeCols(prefix: String, json: String): DataFrame = {
    val df = createDfFromJson(spark, json)
      .withColumn(s"${prefix}_created_time", col(s"${prefix}_created_time").cast(TimestampType))

    if (df.columns.contains(s"${prefix}_updated_time")) {
      df.withColumn(s"${prefix}_updated_time", col(s"${prefix}_updated_time").cast(TimestampType))
    } else df
  }
}
