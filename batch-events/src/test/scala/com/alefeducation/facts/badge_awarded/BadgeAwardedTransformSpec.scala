package com.alefeducation.facts.badge_awarded

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.facts.badge_awarded.BadgeAwardedTransform.BadgeAwardedSource
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class BadgeAwardedTransformSpec extends SparkSuite {

  val sprk: SparkSession = spark
  val service = mock[SparkBatchService]

  val expectedColumns: Set[String] = Set(
    "fba_dw_created_time",
    "fba_date_dw_id",
    "fba_school_id",
    "fba_student_id",
    "fba_id",
    "eventdate",
    "fba_created_time",
    "fba_tier",
    "fba_badge_type_id",
    "fba_tenant_id",
    "fba_academic_year_id",
    "fba_badge_type",
    "fba_grade_id",
    "fba_section_id"
  )

  test("transform badge awarded event successfully") {
    val value = """
                  |[
                  |{
                  |  "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013",
                  |  "eventType": "StudentBadgeAwardedEvent",
                  |  "loadtime": "2023-04-27 10:53:13.675",
                  |	 "id": "457ae0bc-30b7-4ea9-b97b-7d6221dedbdf",
                  |  "studentId": "14e9276f-2197-41bf-a2b2-d8237407cb2d",
                  |	 "badgeType": "MOUNTAIN",
                  |	 "badgeTypeId": "8b8cf877-c6e8-4450-aaaf-2333a92abb3f",
                  |  "tier": "BRONZE",
                  |	 "nextChallengeDescription": "Receive more stars from your teacher to earn a Silver Badge.",
                  |  "awardedMessage": "You have earned a Bronze Badge by receiving awarded stars.",
                  |  "awardDescription": "Receive more stars from your teacher to earn a Bronze Badge.",
                  |  "academicYearId": "0a3a5afd-3b50-447f-a937-7480133bde95",
                  |  "sectionId": "fd605223-dbe9-426e-a8f4-67c76d6357c1",
                  |  "gradeId": "df840323-9ba3-4ffa-a279-464e010fcdd0",
                  |  "schoolId": "e73fa736-59ca-42df-ade6-87558c7df8c2",
                  |  "occurredOn": "2023-04-27 10:01:38.373481108",
                  |  "eventDateDw": "20230427"
                  |}]
        """.stripMargin
    val sprk = spark
    import sprk.implicits._

    val transformer = new BadgeAwardedTransform(sprk, service)

    val inputDF = spark.read.json(Seq(value).toDS())
    when(service.readOptional(BadgeAwardedSource, sprk)).thenReturn(Some(inputDF))
    val df = transformer.transform().get.output

    assert(df.columns.toSet === expectedColumns)
    assert[String](df, "fba_tenant_id", "93e4949d-7eff-4707-9201-dac917a5e013")
    assert[String](df, "fba_id", "457ae0bc-30b7-4ea9-b97b-7d6221dedbdf")
    assert[Long](df, "fba_date_dw_id", 20230427)
    assert[String](df, "fba_school_id", "e73fa736-59ca-42df-ade6-87558c7df8c2")
    assert[String](df, "fba_student_id", "14e9276f-2197-41bf-a2b2-d8237407cb2d")
    assert[String](df, "eventdate", "2023-04-27")
    assert[String](df, "fba_created_time", "2023-04-27 10:01:38.373481")
    assert[String](df, "fba_tier", "BRONZE")
    assert[String](df, "fba_badge_type_id", "8b8cf877-c6e8-4450-aaaf-2333a92abb3f")
    assert[String](df, "fba_academic_year_id", "0a3a5afd-3b50-447f-a937-7480133bde95")
    assert[String](df, "fba_badge_type", "MOUNTAIN")
    assert[String](df, "fba_grade_id", "df840323-9ba3-4ffa-a279-464e010fcdd0")
    assert[String](df, "fba_section_id", "fd605223-dbe9-426e-a8f4-67c76d6357c1")
  }
}
