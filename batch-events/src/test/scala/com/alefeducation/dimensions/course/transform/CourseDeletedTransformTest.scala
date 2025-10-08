package com.alefeducation.dimensions.course.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.course.transform.CourseDeletedTransform.{CourseDeletedParquetSource, CourseDeletedTransformedSink}
import com.alefeducation.dimensions.course.transform.CourseMutatedTransform.CourseDeltaSource
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class CourseDeletedTransformTest extends SparkSuite {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  test("should construct course dataframe from deleted") {
    val value =
      """
        |[
        |{
        |   "eventType": "CourseDeletedEvent",
        |	"id": "88fc9cea-85af-45dd-a8a7-43ef7bf5f598",
        |	"status": "PUBLISHED",
        | "courseType": "pathway",
        |   "occurredOn": "2021-06-23 05:33:24.921",
        |   "courseStatus" : "PUBLISHED"
        |},
        |{
        |   "eventType": "CourseDeletedEvent",
        |	"id": "88fc9cea-85af-45dd-a8a7-43ef7bf5f598",
        |	"status": "IN_REVIEW",
        | "courseType": "pathway",
        |   "occurredOn": "2021-06-22 05:33:24.921",
        |   "courseStatus" : "IN_REVIEW"
        |},
        |{
        |   "eventType": "CourseDeletedEvent",
        |	"id": "88fc9cea-85af-45dd-a8a7-43ef7bf5f598",
        | "courseType": "pathway",
        |	"status": "DRAFT",
        |   "occurredOn": "2021-06-21 05:33:24.921",
        |   "courseStatus" : "DRAFT"
        |},
        |{
        |   "eventType": "CourseDeletedEvent",
        |	"id": "88fc9cea-85af-45dd-a8a7-43ef7bf5f598",
        | "courseType": "pathway",
        |	"status": "PUBLISHED",
        |   "occurredOn": "2021-06-21 05:33:24.921"
        |},
        |{
        |}
        |]
        """.stripMargin

    val deltaExistingValues =
      """
        |[
        |{
        | "course_id": "88fc9cea-85af-45dd-a8a7-43ef7bf5f598",
        | "course_name": "Test events",
        | "course_code": "Test events code",
        | "course_organization": "org",
        | "course_content_repository": null,
        | "course_description": "descr_old",
        | "course_goal": "goal_old",
        | "course_status": 2,
        | "course_created_time": "2021-06-23 00:11:00.111",
        | "course_dw_created_time": "2021-06-23 00:11:00.111",
        | "course_dw_updated_time": null,
        | "course_updated_time": null,
        | "course_deleted_time": null
        |},
        |{
        | "course_id": "88fc9cea-85af-45dd-a8a7-43ef7bf5f598",
        | "course_name": "Test events",
        | "course_code": "Test events code",
        | "course_organization": "org",
        | "course_content_repository": "first",
        | "course_description": "descr",
        | "course_goal": "goal",
        | "course_status": 1,
        | "course_created_time": "2021-06-23 00:11:00.111",
        | "course_dw_created_time": "2021-06-23 00:11:00.111",
        | "course_dw_updated_time": null,
        | "course_updated_time": null,
        | "course_deleted_time": null
        |},
        |{
        | "course_id": "88fc9cea-85af-45dd-a8a7-43ef7bf5f598",
        | "course_name": "Test events",
        | "course_code": "Test events code",
        | "course_organization": "org",
        | "course_content_repository": "second",
        | "course_description": "descr",
        | "course_goal": "goal",
        | "course_status": 1,
        | "course_created_time": "2021-06-23 00:11:00.111",
        | "course_dw_created_time": "2021-06-23 00:11:00.111",
        | "course_dw_updated_time": null,
        | "course_updated_time": null,
        | "course_deleted_time": null
        |}
        |]
        """.stripMargin

    val expectedColumns = Set(
      "course_id",
      "course_name",
      "course_code",
      "course_organization",
      "course_content_repository",
      "course_description",
      "course_goal",
      "course_status",
      "course_created_time",
      "course_dw_created_time",
      "course_dw_updated_time",
      "course_updated_time",
      "course_deleted_time",
      "course_type",
      "rel_course_dw_id"
    )

    val sprk = spark
    import sprk.implicits._

    val inputDF = spark.read.json(Seq(value).toDS())
    when(
      service.readUniqueOptional(
        CourseDeletedParquetSource,
        sprk,
        extraProps = List(("mergeSchema", "true")),
        uniqueColNames = List("id")
      )
    ).thenReturn(Some(inputDF))

    val deltaDF = spark.read.json(Seq(deltaExistingValues).toDS())
    when(service.readOptional(CourseDeltaSource, sprk)).thenReturn(Some(deltaDF))

    val transformer = new CourseDeletedTransform(sprk, service)
    val sinks = transformer.transform()

    val df = sinks.filter(_.get.name == CourseDeletedTransformedSink).head.get.output

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 2)

    assert[String](df, "course_id", "88fc9cea-85af-45dd-a8a7-43ef7bf5f598")
    assert[String](df, "course_name", "Test events")
    assert[String](df, "course_code", "Test events code")
    assert[String](df, "course_organization", "org")
    assert[String](df, "course_content_repository", "first")
    assert[String](df, "course_description", "descr")
    assert[String](df, "course_goal", "goal")
    assert[Int](df, "course_status", 4)
    assert[String](df, "course_created_time", "2021-06-23 05:33:24.921")
    assert[String](df, "course_deleted_time", "2021-06-23 05:33:24.921")
  }

}
