package com.alefeducation.dimensions.generic_transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.BaseDimensionSpec
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class EBookProgressFactTransformSpec extends SparkSuite with BaseDimensionSpec {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  test("should construct transformation dataframe for fact jobs with simple mapping inside configuration") {
    val updatedValue =
      """
        |[
        |{
        |  "eventType":"EbookProgressEvent",
        |  "id": "aec98b4d-0d01-4f94-b22e-0a1e7af49f07",
        |  "action": "PROGRESS",
        |  "tenantId": "tenant-uuid",
        |  "studentId": "c16d3b48-9fb4-4468-8d13-ba940067bc4b",
        |  "lessonId": "2ec15b0a-1dc9-49e9-962c-000000028475",
        |  "contentId": "acec2bcc-b7e9-4d1a-86b4-94c844c5bc2b",
        |  "contentHash": "abc123def456",
        |  "schoolId": "ac6d3b48-9fb4-4468-8d13-ba940067bce1",
        |  "gradeId": "cb6d3b48-9fb4-4468-8d13-ba940067bcea",
        |  "classId": "c133d3b48-9fb4-4468-8d13-ba940067bcea",
        |  "materialType": "PATHWAY",
        |  "academicYearTag": "2023-2024",
        |  "experienceId": "ac6d3b48-9fb4-4468-8d13-ba940067bce4",
        |  "sessionId": "dc6d3b48-9fb4-4468-8d13-ba940067bce2",
        |  "ebookMetaData": {
        |    "title": "The Jungle Book",
        |    "totalPages": 52,
        |    "hasAudio": true
        |  },
        |  "progressData": {
        |    "isLastPage": false,
        |    "location": "epubcfi(/6/12!/4/2/8/2[f000001])",
        |    "status": "IN_PROGRESS",
        |    "timeSpent": 102839
        |  },
        |  "bookmark": {
        |      "location": "epubcfi(/6/12!/4/2/8/2[f000001])"
        |  },
        |  "highlight": {
        |      "location": "epubcfi(/6/12!/4/2/8/2[f000001])"
        |  },
        |  "occurredOn": "2021-06-23 05:33:25.921"
        |}
        |]
        |""".stripMargin

    val expectedColumns = Set(
      "fep_dw_id",
      "fep_id",
      "fep_session_id",
      "fep_exp_id",
      "fep_student_id",
      "feb_ay_tag",
      "fep_tenant_id",
      "fep_school_id",
      "fep_grade_id",
      "fep_class_id",
      "fep_lo_id",
      "fep_step_instance_step_id",
      "fep_content_hash",
      "fep_material_type",
      "fep_title",
      "fep_total_pages",
      "fep_has_audio",
      "fep_action",
      "fep_is_last_page",
      "fep_location",
      "fep_state",
      "fep_time_spent",
      "fep_bookmark_location",
      "fep_highlight_location",
      "fep_created_time",
      "fep_dw_created_time",
      "fep_date_dw_id",
      "eventdate"
    )

    val sprk = spark
    import sprk.implicits._

    val transformer = new GenericFactTransform(sprk, service, "ebook-progress-transform")

    val inputDf = spark.read.json(Seq(updatedValue).toDS())
    when(service.readOptional("parquet-ebook-progress-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(inputDf))
    when(service.getStartIdUpdateStatus("fact_ebook_progress")).thenReturn(1001)

    val sinks = transformer.transform()

    val df = sinks.head.output

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 1)
    assert[String](df, "fep_id", "aec98b4d-0d01-4f94-b22e-0a1e7af49f07")
    assert[String](df, "fep_session_id", "dc6d3b48-9fb4-4468-8d13-ba940067bce2")
    assert[String](df, "fep_exp_id", "ac6d3b48-9fb4-4468-8d13-ba940067bce4")
    assert[String](df, "fep_student_id", "c16d3b48-9fb4-4468-8d13-ba940067bc4b")
    assert[String](df, "feb_ay_tag", "2023-2024")
    assert[String](df, "fep_school_id", "ac6d3b48-9fb4-4468-8d13-ba940067bce1")
    assert[String](df, "fep_grade_id", "cb6d3b48-9fb4-4468-8d13-ba940067bcea")
    assert[String](df, "fep_class_id", "c133d3b48-9fb4-4468-8d13-ba940067bcea")
    assert[String](df, "fep_lo_id", "2ec15b0a-1dc9-49e9-962c-000000028475")
    assert[String](df, "fep_step_instance_step_id", "acec2bcc-b7e9-4d1a-86b4-94c844c5bc2b")
    assert[String](df, "fep_content_hash", "abc123def456")
    assert[String](df, "fep_material_type", "PATHWAY")
    assert[String](df, "fep_title", "The Jungle Book")
    assert[Int](df, "fep_total_pages", 52)
    assert[Boolean](df, "fep_has_audio", true)
    assert[String](df, "fep_action", "PROGRESS")
    assert[Boolean](df, "fep_is_last_page", false)
    assert[String](df, "fep_location", "epubcfi(/6/12!/4/2/8/2[f000001])")
    assert[String](df, "fep_state", "IN_PROGRESS")
    assert[Int](df, "fep_time_spent", 102839)
    assert[String](df, "fep_bookmark_location", "epubcfi(/6/12!/4/2/8/2[f000001])")
    assert[String](df, "fep_highlight_location", "epubcfi(/6/12!/4/2/8/2[f000001])")
    assert[String](df, "fep_created_time", "2021-06-23 05:33:25.921")
    assert[String](df, "fep_date_dw_id", "20210623")
    assert[String](df, "eventdate", "2021-06-23")
  }
}
