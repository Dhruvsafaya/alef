package com.alefeducation.dimensions.course_activity_association.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.course_activity_association.transform.CourseActivityOutcomeAssociationTransform.{
  CourseActivityOutcomeAssociationService,
  MappedOutcomesColumnName
}
import com.alefeducation.util.DataFrameEqualityUtils.{assertSmallDatasetEquality, createDfFromJson}
import com.alefeducation.util.Resources.{getSink, getSource}
import org.apache.spark.sql.functions.{col, from_json, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class CourseActivityOutcomeAssociationTransformSpec extends SparkSuite {

  private val service: SparkBatchService = mock[SparkBatchService]

  private val ExpectedCourseActivityOutcomeAssociationColumns: Set[String] = Set(
    "caoa_dw_id",
    "caoa_course_id",
    "caoa_activity_id",
    "caoa_outcome_id",
    "caoa_outcome_type",
    "caoa_curr_id",
    "caoa_curr_grade_id",
    "caoa_curr_subject_id",
    "caoa_status",
    "caoa_created_time",
    "caoa_updated_time",
    "caoa_dw_created_time",
    "caoa_dw_updated_time",
  )

  private object Sources {
    val ActivityPlannedSource = "parquet-published-activity-planned-in-courses-source"
    val ActivityUpdatedSource = "parquet-published-activity-updated-in-courses-source"
  }

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

  val NullOutcome = "null"

  val EmptyOutcome: String =
    """
      |[]
      |""".stripMargin

  val Outcome: String =
    """
      |[
      |  {
      |    "id": 2,
      |    "type": "Foo",
      |    "curriculumId": 22,
      |    "gradeId": 222,
      |    "subjectId": 2222
      |  }
      |]
      |""".stripMargin

  test("should create activity association outcomes dataframe") {

    val expectedJson: String =
      """
        |[
        |{"caoa_dw_id": 2, "caoa_course_id":"course_id1","caoa_status":1,"caoa_curr_subject_id":2222,"caoa_outcome_id":"2","caoa_activity_id":"activity_id1","caoa_outcome_type":"Foo","caoa_curr_grade_id":222,"caoa_curr_id":22,"caoa_created_time":"2023-01-23T06:34:09.319Z","caoa_dw_created_time":"2023-07-18T09:41:30.052Z","caoa_dw_updated_time":null},
        |{"caoa_dw_id": 1, "caoa_course_id":"course_id1","caoa_status":2,"caoa_curr_subject_id":null,"caoa_outcome_id":null,"caoa_activity_id":"activity_id1","caoa_outcome_type":null,"caoa_curr_grade_id":null,"caoa_curr_id":null,"caoa_created_time":"2023-01-23T06:32:09.319Z","caoa_dw_created_time":"2023-07-18T09:41:30.052Z","caoa_updated_time":"2023-01-23T06:34:09.319Z","caoa_dw_updated_time":null}
        |]
        |""".stripMargin

    val sinkName = getSink(CourseActivityOutcomeAssociationService).head

    when(service.getStartIdUpdateStatus("dim_course_activity_outcome_association")).thenReturn(1)

    val sprk: SparkSession = spark
    import sprk.implicits._

    val activityPlanned =
      spark.read
        .json(Seq(ActivityPlannedMsg).toDS())
        .transform(addLearningOutcome(NullOutcome))
    when(service.readOptional(Sources.ActivityPlannedSource, sprk)).thenReturn(Some(activityPlanned))

    val activityUpdated =
      spark.read
        .json(Seq(ActivityUpdatedMsg).toDS())
        .transform(addLearningOutcome(Outcome))
    when(service.readOptional(Sources.ActivityUpdatedSource, sprk)).thenReturn(Some(activityUpdated))

    val transformer = new CourseActivityOutcomeAssociationTransform(spark, service)

    val sinks = transformer.transform(CourseActivityOutcomeAssociationService)
    val df = sinks.filter(_.name === sinkName).head.output

    assert(df.columns.toSet === ExpectedCourseActivityOutcomeAssociationColumns)
    val expDf = createDfFromJsonWithTimeCols("caoa", expectedJson)
    assertSmallDatasetEquality("caoa", df, expDf)
  }

  private def addLearningOutcome(json: String)(df: DataFrame): DataFrame =
    if (json == "null") {
      df.withColumn(MappedOutcomesColumnName, lit(null))
    } else {
      df.withColumn(MappedOutcomesColumnName, from_json(lit(json), LearningOutcomesSchema))
    }

  val LearningOutcomesSchema: ArrayType = ArrayType(
    StructType(
      Seq(
        StructField("id", LongType),
        StructField("type", StringType),
        StructField("curriculumId", LongType),
        StructField("gradeId", LongType),
        StructField("subjectId", LongType)
      )
    )
  )
  private def createDfFromJsonWithTimeCols(prefix: String, json: String): DataFrame = {
    val df = createDfFromJson(spark, json)
      .withColumn(s"${prefix}_created_time", col(s"${prefix}_created_time").cast(TimestampType))

    if (df.columns.contains(s"${prefix}_updated_time")) {
      df.withColumn(s"${prefix}_updated_time", col(s"${prefix}_updated_time").cast(TimestampType))
    } else df
  }
}
