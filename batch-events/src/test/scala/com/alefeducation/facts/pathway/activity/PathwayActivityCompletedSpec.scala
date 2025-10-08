package com.alefeducation.facts.pathway.activity

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.facts.pathway.activity.PathwayActivityCompletedTransform.ParquetPathwayActivityCompletedSource
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class PathwayActivityCompletedSpec extends SparkSuite {
  val sprk: SparkSession = spark
  val service = mock[SparkBatchService]

  val expectedColumns = Set(
    "fpac_created_time",
    "fpac_dw_created_time",
    "fpac_date_dw_id",
    "fpac_student_id",
    "fpac_class_id",
    "fpac_pathway_id",
    "fpac_level_id",
    "fpac_tenant_id",
    "fpac_activity_id",
    "fpac_activity_type",
    "fpac_score",
    "fpac_time_spent",
    "fpac_learning_session_id",
    "fpac_attempt",
    "eventdate",
    "fpac_academic_year"
  )

  test("transform pathway activity completed event successfully") {

    val value = """
                  |[
                  |{
                  |  "eventType": "PathwayActivityCompletedEvent",
                  |  "learnerId": "student-id-1",
                  |	 "classId": "class-id-1",
                  |	 "pathwayId": "pathway-id-1",
                  |	 "levelId": "level-id-1",
                  |	 "activityId": "activity-id-1",
                  |	 "activityType": "ACTIVITY",
                  |	 "score": 20.0,
                  |	 "occurredOn": "2022-09-22T05:20:00.914",
                  |  "eventDateDw": 20220922,
                  |  "tenantId": "tenant-id",
                  |  "timespent": 24,
                  |  "attempt": 1,
                  |  "academicYear": "2023-2024",
                  |  "learningSessionId": "session-id-1"
                  |}]
        """.stripMargin
    val sprk = spark
    import sprk.implicits._
    val transformer = new PathwayActivityCompletedTransform(sprk, service)
    val inputDF = spark.read.json(Seq(value).toDS())
    when(service.readOptional(ParquetPathwayActivityCompletedSource, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(inputDF))
    val df = transformer.transform().get.output

    assert(df.columns.toSet === expectedColumns)
    assert[String](df, "fpac_created_time", "2022-09-22 05:20:00.914")
    assert[String](df, "fpac_date_dw_id", "20220922")
    assert[String](df, "fpac_student_id", "student-id-1")
    assert[String](df, "fpac_class_id", "class-id-1")
    assert[String](df, "fpac_pathway_id", "pathway-id-1")
    assert[String](df, "fpac_level_id", "level-id-1")
    assert[String](df, "fpac_activity_id", "activity-id-1")
    assert[String](df, "fpac_tenant_id", "tenant-id")
    assert[String](df, "fpac_academic_year", "2023-2024")
    assert[Int](df, "fpac_activity_type", 1)
    assert[Int](df, "fpac_attempt", 1)
    assert[String](df, "fpac_learning_session_id", "session-id-1")
    assert[Double](df, "fpac_score", 20.0)
  }

}
