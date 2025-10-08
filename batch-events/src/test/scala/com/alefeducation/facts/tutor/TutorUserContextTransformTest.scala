package com.alefeducation.facts.tutor

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.facts.tutor.TutorUserContextTransform.ParquetTutorUserContextSource
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class TutorUserContextTransformTest extends SparkSuite {
  val sprk: SparkSession = spark
  val service = mock[SparkBatchService]

  val expectedColumns: Set[String] = Set(
    "ftc_role",
    "ftc_context_id",
    "ftc_school_id",
    "ftc_grade_id",
    "ftc_grade",
    "ftc_subject_id",
    "ftc_subject",
    "ftc_language",
    "eventdate",
    "ftc_tenant_id",
    "ftc_created_time",
    "ftc_dw_created_time",
    "ftc_date_dw_id",
    "ftc_user_id",
    "ftc_tutor_locked"
  )

  test("should transform tutor user context event successfully") {
    val value = """
                  |[
                  |{
                  |"tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
                  |"eventType":"UserContextUpdated",
                  |"loadtime":"2023-05-17T05:39:28.481Z",
                  |"role":"student",
                  |"grade":3,
                  |"subject":"Math",
                  |"language":"english",
                  |"schoolId": "457ae0bc-30b7-4ea9-b97b-7d6221dedbdf",
                  |"gradeId": "e73fa736-59ca-42df-ade6-87558c7df8c2",
                  |"subjectId": "123467",
                  |"contextId": "df840323-9ba3-4ffa-a279-464e010fcdd0",
                  |"userId": "fd605223-dbe9-426e-a8f4-67c76d6357c1",
                  |"occurredOn": "2023-04-27 10:01:38.373481108",
                  |"eventDateDw": "20230427",
                  |"tutorLocked": false
                  |}
                  |]
        """.stripMargin
    val sprk = spark
    import sprk.implicits._

    val transformer = new TutorUserContextTransform(sprk, service)

    val inputDF = spark.read.json(Seq(value).toDS())
    when(service.readOptional(ParquetTutorUserContextSource, sprk)).thenReturn(Some(inputDF))
    val df = transformer.transform().get.output

    assert(df.columns.toSet === expectedColumns)
    assert[String](df, "ftc_role", "student")
    assert[String](df, "ftc_context_id", "df840323-9ba3-4ffa-a279-464e010fcdd0")
    assert[String](df, "ftc_school_id", "457ae0bc-30b7-4ea9-b97b-7d6221dedbdf")
    assert[String](df, "ftc_grade_id", "e73fa736-59ca-42df-ade6-87558c7df8c2")
    assert[String](df, "eventdate", "2023-04-27")
    assert[String](df, "ftc_created_time", "2023-04-27 10:01:38.373481")
    assert[Int](df, "ftc_grade", 3)
    assert[Long](df, "ftc_subject_id", 123467)
    assert[String](df, "ftc_subject", "Math")
    assert[String](df, "ftc_language", "english")
    assert[String](df, "ftc_tenant_id", "93e4949d-7eff-4707-9201-dac917a5e013")
    assert[String](df, "ftc_user_id", "fd605223-dbe9-426e-a8f4-67c76d6357c1")
    assert[Boolean](df, "ftc_tutor_locked", false)
  }
}
