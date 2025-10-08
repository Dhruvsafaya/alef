package com.alefeducation.facts.pathway.leaderboard

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.facts.pathway.leaderboard.PathwayLeaderboardTransform.PathwayLeaderboardEntity
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

import java.sql.Date

class PathwayLeaderboardTransformSpec extends SparkSuite {

  val sprk: SparkSession = spark
  val service = mock[SparkBatchService]

  val expectedColumns = Set(
    "fpl_start_date",
    "fpl_class_id",
    "fpl_total_stars",
    "fpl_created_time",
    "fpl_average_score",
    "eventdate",
    "fpl_date_dw_id",
    "fpl_tenant_id",
    "fpl_student_id",
    "fpl_level_competed_count",
    "fpl_id",
    "fpl_order",
    "fpl_grade_id",
    "fpl_end_date",
    "fpl_dw_created_time",
    "fpl_pathway_id",
    "fpl_academic_year_id"
  )

  test("transform pathway leaderboard event successfully") {

    val value =
      """
        |{
        |   "eventType": "PathwayLeaderboardUpdatedEvent",
        |   "tenantId": "efbac188-71da-4145-9388-a2d49b383784",
        |	"id": "b38240e2-1c91-47db-abe7-37d74c589db7",
        |	"type": "PATHWAY",
        |	"classId": "33e5e113-fb5e-4341-9e27-a3efe3d00b88",
        |	"pathwayId": "feb981fd-ca5b-45bf-95de-2f02a8f95e64",
        |	"gradeId": "9184acd2-02f7-49e5-9f4e-f0c034188e6b",
        |	"className": "Dragons-Pathway-DNT",
        |	"academicYearId": "3d47b897-830b-45f5-b5da-fbcfbad9541e",
        |	"startDate": "2023-06-20",
        |	"endDate": "2023-06-26",
        |	"leaders": [
        |		{
        |			"studentId": "8a3335dd-c6ba-468d-8453-111d847d8cf0",
        |			"name": "Dragons-e2e-s11  Dragons-e2e-s11",
        |			"avatar": "avatar_48",
        |			"order": 1,
        |			"progress": 1,
        |			"averageScore": 73,
        |			"totalStars": 6
        |		},
        |       {
        |			"studentId": "904cd54c-d4e9-463c-b325-49410d14de09",
        |			"name": "Dragons-e2e-s12  Dragons-e2e-s12",
        |			"avatar": "avatar_49",
        |			"order": 1,
        |			"progress": 2,
        |			"averageScore": 74,
        |			"totalStars": 7
        |		}
        |	],
        |	"occurredOn": "2023-06-20T05:20:00.914"
        |}
        |""".stripMargin

    val sprk = spark
    import sprk.implicits._

    val inputDF = spark.read.json(Seq(value).toDS())
    when(service.readOptional("parquet-pathway-leaderboard-source", sprk)).thenReturn(Some(inputDF))

    val transformer = new PathwayLeaderboardTransform(sprk, service)

    val df = transformer.transform().get.output

    assert(df.columns.toSet === expectedColumns)
    assert[String](df, s"${PathwayLeaderboardEntity}_tenant_id", "efbac188-71da-4145-9388-a2d49b383784")
    assert[String](df, s"${PathwayLeaderboardEntity}_id", "b38240e2-1c91-47db-abe7-37d74c589db7")
    assert[String](df, s"${PathwayLeaderboardEntity}_class_id", "33e5e113-fb5e-4341-9e27-a3efe3d00b88")
    assert[String](df, s"${PathwayLeaderboardEntity}_academic_year_id", "3d47b897-830b-45f5-b5da-fbcfbad9541e")
    assert[String](df, s"${PathwayLeaderboardEntity}_grade_id", "9184acd2-02f7-49e5-9f4e-f0c034188e6b")
    assert[Date](df, s"${PathwayLeaderboardEntity}_start_date", Date.valueOf("2023-06-20"))
    assert[Date](df, s"${PathwayLeaderboardEntity}_end_date", Date.valueOf("2023-06-26"))
    assert[String](df, s"${PathwayLeaderboardEntity}_created_time", "2023-06-20 05:20:00.914")
    assert[String](df, s"eventdate", "2023-06-20")

    val lst = getStudentData(df)

    assert(lst.toSet === Set(
        ("8a3335dd-c6ba-468d-8453-111d847d8cf0", 1, 1, 73, 6),
        ("904cd54c-d4e9-463c-b325-49410d14de09", 1, 2, 74, 7)
      )
    )
  }

  private def getStudentData(df: DataFrame) = {
    df.select(s"${PathwayLeaderboardEntity}_student_id",
      s"${PathwayLeaderboardEntity}_order",
      s"${PathwayLeaderboardEntity}_level_competed_count",
      s"${PathwayLeaderboardEntity}_average_score",
      s"${PathwayLeaderboardEntity}_total_stars").collect().map { row =>
      (row.getAs[String](s"${PathwayLeaderboardEntity}_student_id"),
        row.getAs[Int](s"${PathwayLeaderboardEntity}_order"),
        row.getAs[Int](s"${PathwayLeaderboardEntity}_level_competed_count"),
        row.getAs[Int](s"${PathwayLeaderboardEntity}_average_score"),
        row.getAs[Int](s"${PathwayLeaderboardEntity}_total_stars")
      )
    }
  }
}
