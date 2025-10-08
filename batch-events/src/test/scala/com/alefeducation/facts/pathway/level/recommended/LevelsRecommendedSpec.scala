package com.alefeducation.facts.pathway.level.recommended

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.facts.pathway.level.recommended.LevelsRecommendedTransform.ParquetLevelsRecommendedSource
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class LevelsRecommendedSpec extends SparkSuite {
  val sprk: SparkSession = spark
  val service = mock[SparkBatchService]

  val expectedColumns = Set(
    "flr_created_time",
    "flr_dw_created_time",
    "flr_date_dw_id",
    "flr_recommended_on",
    "flr_tenant_id",
    "flr_student_id",
    "flr_class_id",
    "flr_pathway_id",
    "flr_completed_level_id",
    "flr_level_id",
    "flr_status",
    "flr_recommendation_type",
    "eventdate",
    "flr_academic_year"
  )

  test("transform levels recommended event successfully") {

    val value =
      """
        |[
        |{
        |  "eventType": "LevelsRecommendedEvent",
        |  "learnerId": "student-id-1",
        |	 "classId": "class-id-1",
        |	 "pathwayId": "pathway-id-1",
        |	 "completedLevelId": null,
        |	 "recommendedLevels": [
        |	  	{
        |	  		"id": "level-id-1",
        |	  		"name": "Level-1",
        |	  		"status": "ACTIVE"
        |	  	},
        |	  	{
        |	  		"id": "level-id-2",
        |	  		"name": "Level-2",
        |	  		"status": "ACTIVE"
        |	  	},
        |	  	{
        |	  		"id": "level-id-3",
        |	  		"name": "Level-3",
        |	  		"status": "ACTIVE"
        |	  	}
        |	  ],
        |	  "recommendedOn": "2022-09-22T10:32:34.609",
        |	  "recommendationType": "PLACEMENT_COMPLETION",
        |   "eventDateDw": 20220922,
        |	  "occurredOn": "2022-09-22T10:32:34.611",
        |   "academicYear": "2024-2024",
        |   "tenantId": "tenant-id"
        |}]
        """.stripMargin
    val sprk = spark
    import sprk.implicits._
    val transformer = new LevelsRecommendedTransform(sprk, service)
    val inputDF = spark.read.json(Seq(value).toDS())
    when(service.readOptional(ParquetLevelsRecommendedSource, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(inputDF))
    val df = transformer.transform().get.output

    assert(df.columns.toSet === expectedColumns)
    assert[String](df, "flr_completed_level_id", null)
    assert[String](df, "flr_level_id", "level-id-1")
    assert[String](df, "flr_created_time", "2022-09-22 10:32:34.611")
    assert[String](df, "flr_date_dw_id", "20220922")
    assert[String](df, "flr_recommended_on", "2022-09-22 10:32:34.609")
    assert[String](df, "flr_tenant_id", "tenant-id")
    assert[String](df, "flr_student_id", "student-id-1")
    assert[String](df, "flr_class_id", "class-id-1")
    assert[String](df, "flr_pathway_id", "pathway-id-1")
    assert[String](df, "flr_academic_year", "2024-2024")
    assert[Int](df, "flr_status", 1)
    assert[Int](df, "flr_recommendation_type", 1)

  }

}
