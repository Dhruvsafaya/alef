package com.alefeducation.facts.pathway.level.completed

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.facts.pathway.level.completed.LevelCompletedTransform._
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class LevelCompletedSpec extends SparkSuite {
  val sprk: SparkSession = spark
  val service = mock[SparkBatchService]

  val expectedColumns = Set(
    "flc_created_time",
    "flc_dw_created_time",
    "flc_completed_on",
    "flc_tenant_id",
    "flc_student_id",
    "flc_class_id",
    "flc_pathway_id",
    "flc_level_id",
    "flc_total_stars",
    "flc_date_dw_id",
    "eventdate",
    "flc_academic_year",
    "flc_score"
  )

  test("transform level completed event successfully") {

    val value =
      """
        |[{
        |  "eventType": "LevelCompletedEvent",
        |	 "learnerId": "student-id-1",
        |	 "classId": "class-id-1",
        |	 "pathwayId": "pathway-id-1",
        |	 "levelId": "level-id-1",
        |	 "levelName": "Level-4",
        |	 "totalStars": 8,
        |	 "completedOn": "2022-09-22T10:17:04.317",
        |	 "occurredOn": "2022-09-22T10:17:04.318",
        |  "eventDateDw": 20220922,
        |  "academicYear": "2023-2024",
        |  "tenantId": "tenant-id"
        |}]
        """.stripMargin

    val sprk = spark
    import sprk.implicits._
    val transformer = new LevelCompletedTransform(sprk, service)
    val inputDF = spark.read.json(Seq(value).toDS())
    when(service.readOptional(ParquetLevelCompletedSource, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(inputDF))
    val df = transformer.transform().get.output

    assert(df.columns.toSet === expectedColumns)
    assert[String](df, "flc_created_time", "2022-09-22 10:17:04.318")
    assert[String](df, "flc_completed_on", "2022-09-22 10:17:04.317")
    assert[String](df, "flc_student_id", "student-id-1")
    assert[String](df, "flc_class_id", "class-id-1")
    assert[String](df, "flc_pathway_id", "pathway-id-1")
    assert[String](df, "flc_level_id", "level-id-1")
    assert[String](df, "flc_tenant_id", "tenant-id")
    assert[String](df, "flc_academic_year", "2023-2024")
    assert[Int](df, "flc_total_stars", 8)
    assert[String](df, "flc_date_dw_id", "20220922")
    assert[String](df, "eventdate", "2022-09-22")
  }

}
