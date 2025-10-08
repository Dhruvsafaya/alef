package com.alefeducation.facts.tutor

import com.alefeducation.base.SparkBatchService
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.facts.tutor.TutorSessionTransform.ParquetTutorSessionSource

class TutorSessionTransformTest extends SparkSuite {
  val sprk: SparkSession = spark
  val service = mock[SparkBatchService]

  val expectedColumns: Set[String] = Set(
    "eventdate",
    "fts_date_dw_id",
    "fts_created_time",
    "fts_dw_created_time",
    "fts_user_id",
    "fts_role",
    "fts_context_id",
    "fts_school_id",
    "fts_grade_id",
    "fts_grade",
    "fts_subject_id",
    "fts_subject",
    "fts_language",
    "fts_session_state",
    "fts_material_id",
    "fts_material_type",
    "fts_activity_id",
    "fts_activity_status",
    "fts_level_id",
    "fts_outcome_id",
    "fts_tenant_id",
    "fts_session_id",
    "fts_session_message_limit_reached",
    "fts_learning_session_id"
  )

  test("should transform tutor session event successfully") {
    val value = """
                  |[
                  |{
                  |"tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
                  |"eventType":"ChatSessionCreated",
                  |"loadtime":"2023-05-17T05:39:28.481Z",
                  |"role":"student",
                  |"grade":3,
                  |"subject":"Math",
                  |"language":"english",
                  |"schoolId": "457ae0bc-30b7-4ea9-b97b-7d6221dedbdf",
                  |"gradeId": "e73fa736-59ca-42df-ade6-87558c7df8c2",
                  |"subjectId": "136789",
                  |"contextId": "df840323-9ba3-4ffa-a279-464e010fcdd0",
                  |"userId": "fd605223-dbe9-426e-a8f4-67c76d6357c1",
                  |"occurredOn": "2023-04-27 10:01:38.373481108",
                  |"sessionStatus": "finished",
                  |"sessionId": "s57ae0bc-30b7-4ea9-b97b-7d6221dedbd",
                  |"materialId": "m57ae0bc-30b7-4ea9-b97b-7d6221dedbd",
                  |"materialType": "pathways",
                  |"activityId": "a57ae0bc-30b7-4ea9-b97b-7d6221dedbd",
                  |"activityStatus": "in_progress",
                  |"levelId": "l57ae0bc-30b7-4ea9-b97b-7d6221dedbd",
                  |"outcomeId": "o57ae0bc-30b7-4ea9-b97b-7d6221dedbd",
                  |"eventDateDw": "20230427",
                  |"sessionMessageLimitReached": false,
                  |"learningSessionId": "d040eec0-1166-45e8-ba6c-a174eedfa8b4"
                  |}
                  |]
        """.stripMargin
    val sprk = spark
    import sprk.implicits._

    val transformer = new TutorSessionTransform(sprk, service)

    val inputDF = spark.read.json(Seq(value).toDS())
    when(service.readOptional(ParquetTutorSessionSource, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(inputDF))
    val df = transformer.transform().get.output

    assert(df.columns.toSet === expectedColumns)
    assert[String](df, "fts_role", "student")
    assert[String](df, "fts_context_id", "df840323-9ba3-4ffa-a279-464e010fcdd0")
    assert[String](df, "fts_school_id", "457ae0bc-30b7-4ea9-b97b-7d6221dedbdf")
    assert[String](df, "fts_grade_id", "e73fa736-59ca-42df-ade6-87558c7df8c2")
    assert[String](df, "eventdate", "2023-04-27")
    assert[String](df, "fts_created_time", "2023-04-27 10:01:38.373481")
    assert[Int](df, "fts_grade", 3)
    assert[Long](df, "fts_subject_id", 136789)
    assert[String](df, "fts_subject", "Math")
    assert[String](df, "fts_language", "english")
    assert[String](df, "fts_tenant_id", "93e4949d-7eff-4707-9201-dac917a5e013")
    assert[String](df, "fts_user_id", "fd605223-dbe9-426e-a8f4-67c76d6357c1")
    assert[Int](df, "fts_session_state", 3)
    assert[Boolean](df, "fts_session_message_limit_reached", false)
  }
}
