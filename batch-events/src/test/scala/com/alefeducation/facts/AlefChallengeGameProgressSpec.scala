package com.alefeducation.facts

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.BaseDimensionSpec
import com.alefeducation.dimensions.generic_transform.GenericFactTransform
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class AlefChallengeGameProgressSpec extends SparkSuite with BaseDimensionSpec {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  test("should construct transformation data frame for fact jobs with simple mapping inside configuration") {
    val updatedValue =
      """
        |[{
        |       "eventType":"AlefGameChallengeProgressEvent",
        |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
        |       "id": "id1",
        |       "state": "JOINED",
        |       "studentId": "studentId1",
        |       "gameId": "gameId1",
        |       "schoolId": "schoolId1",
        |       "grade": "1",
        |       "score": 0,
        |       "organization": "organization1",
        |       "academicYearTag": "academicYearTag1",
        |       "academicYearId": "academicYearId1",
        |       "gameConfig": "{}",
        |       "occurredOn": "2023-05-15 16:23:46.609"
        |}]
        |""".stripMargin

    val expectedColumns = Set(
      "fgc_dw_id",
      "fgc_id",
      "fgc_game_id",
      "fgc_state",
      "fgc_tenant_id",
      "fgc_student_id",
      "fgc_academic_year_id",
      "fgc_academic_year_tag",
      "fgc_school_id",
      "fgc_grade",
      "fgc_organization",
      "fgc_score",
      "fgc_created_time",
      "fgc_dw_created_time",
      "fgc_date_dw_id",
      "eventdate"
    )

    val sprk = spark
    import sprk.implicits._

    val transformer = new GenericFactTransform(sprk, service, "alef-challenge-game-progress-transform")

    val inputDf = spark.read.json(Seq(updatedValue).toDS())
    when(service.readOptional("parquet-alef-challenge-game-progress-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(inputDf))
    when(service.getStartIdUpdateStatus("fact_challenge_game_progress")).thenReturn(1001)

    val sinks = transformer.transform()

    val df = sinks.head.output

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 1)
    assert[String](df, "fgc_id", "id1")
    assert[String](df, "fgc_game_id", "gameId1")
    assert[String](df, "fgc_state", "JOINED")
    assert[String](df, "fgc_tenant_id", "93e4949d-7eff-4707-9201-dac917a5e013")
    assert[String](df, "fgc_student_id", "studentId1")
    assert[String](df, "fgc_academic_year_id", "academicYearId1")
    assert[String](df, "fgc_academic_year_tag", "academicYearTag1")
    assert[String](df, "fgc_school_id", "schoolId1")
    assert[String](df, "fgc_grade", "1")
    assert[String](df, "fgc_organization", "organization1")
    assert[Int](df, "fgc_score", 0)
    assert[String](df, "fgc_created_time", "2023-05-15 16:23:46.609")
    assert[String](df, "fgc_date_dw_id", "20230515")
    assert[String](df, "eventdate", "2023-05-15")
  }
}
