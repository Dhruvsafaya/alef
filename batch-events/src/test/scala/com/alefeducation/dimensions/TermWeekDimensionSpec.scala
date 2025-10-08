package com.alefeducation.dimensions

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.term_week.TermWeekTransform
import com.alefeducation.dimensions.term_week.TermWeekTransform.{TermCreatedSinkName, TermWeekCreatedSourceName, WeekCreatedSinkName, weekColumns}
import com.alefeducation.schema.admin.{ContentRepositoryRedshift, DwIdMapping}
import com.alefeducation.util.Helpers._
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

import java.sql.Date

class TermWeekDimensionSpec extends SparkSuite {

  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  val termExpectedColumns: Set[String] = Set(
    "term_deleted_time", "term_content_repository_id", "term_id", "term_dw_created_time", "term_curriculum_id", "term_start_date", "term_updated_time", "term_end_date", "term_content_academic_year_id", "term_dw_updated_time", "term_status", "term_created_time", "term_academic_period_order", "term_content_repository_dw_id"
  )

  val weekExpectedColumns: Set[String] = Set(
    "week_created_time","week_updated_time","week_deleted_time","week_dw_created_time","week_dw_updated_time","week_status","week_id","week_number","week_start_date","week_end_date","week_term_id"
  )

  test("Test TermCreatedEvent Transform") {

    val termWeekEvent =
      """
        |{
        |  "id": "c58ddf53-caa8-46f5-8281-ded48bd19cf0",
        |  "number": 1,
        |  "curriculumId": 392027,
        |  "academicYearId": 3,
        |  "organisationId": "25e9b735-6b6c-403b-a9f3-95e478e8f1ed",
        |  "contentRepositoryId": "25e9b735-6b6c-403b-a9f3-95e478e8f1ed",
        |  "startDate": "2021-01-02",
        |  "endDate": "2021-01-28",
        |  "weeks": [
        |    {
        |      "id": "badfce75-de01-4cf3-8e48-a0d2799847ac",
        |      "number": 1,
        |      "startDate": "2020-12-27"
        |    },
        |    {
        |      "id": "5586e503-a6da-42fd-9c36-23ee5d9495ed",
        |      "number": 2,
        |      "startDate": "2021-01-03"
        |    },
        |    {
        |      "id": "3253a26a-2451-42c8-b98d-99735e58b8e5",
        |      "number": 3,
        |      "startDate": "2021-01-10"
        |    },
        |    {
        |      "id": "ec3f9ffe-d5e8-4255-804a-ff4cef3a8700",
        |      "number": 4,
        |      "startDate": "2021-01-17"
        |    },
        |    {
        |      "id": "de69b5b5-5741-43e2-b74f-f19005eb4c19",
        |      "number": 5,
        |      "startDate": "2021-01-24"
        |    }
        |  ],
        |  "createdOn": "2020-03-11T07:29:27.321825Z",
        |  "occurredOn": "2020-04-01 05:00:00",
        |  "eventType" : "TermCreatedEvent",
        |  "eventDateDw" : "20200311"
        |}
        |""".stripMargin

    val dwIdMapping =
      """
        |[{
        |  "content_repository_dw_id": 3,
        |  "content_repository_id": "25e9b735-6b6c-403b-a9f3-95e478e8f1ed"
        |}]
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val inputDF = spark.read.json(Seq(termWeekEvent).toDS())
    when(service.readOptional(TermWeekCreatedSourceName, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(inputDF))

    val dwIdDF = spark.read.json(Seq(dwIdMapping).toDS())
    when(service.readFromRedshift[ContentRepositoryRedshift](RedshiftContentRepositorySource,selectedCols = List("content_repository_id", "content_repository_dw_id"))).thenReturn(dwIdDF)

    val transformer = new TermWeekTransform(sprk, service)
    val sinks = transformer.transform()

    val termCreatedTransformedDF = sinks
      .flatten
      .find(_.name == TermCreatedSinkName).get.output

    assert(termCreatedTransformedDF.count() == 1)
    assert(termCreatedTransformedDF.columns.toSet === termExpectedColumns)
    assert[String](termCreatedTransformedDF, "term_id", "c58ddf53-caa8-46f5-8281-ded48bd19cf0")
    assert[Integer](termCreatedTransformedDF, "term_academic_period_order", 1)
    assert[Integer](termCreatedTransformedDF, "term_curriculum_id", 392027)
    assert[Integer](termCreatedTransformedDF, "term_content_academic_year_id", 3)
    assert[Date](termCreatedTransformedDF, "term_start_date", Date.valueOf("2021-01-02"))
    assert[Date](termCreatedTransformedDF, "term_end_date", Date.valueOf("2021-01-28"))
    assert[Integer](termCreatedTransformedDF, "term_status", 1)
    assert[String](termCreatedTransformedDF, "term_content_repository_id", "25e9b735-6b6c-403b-a9f3-95e478e8f1ed")
    assert[Integer](termCreatedTransformedDF, "term_content_repository_dw_id", 3)


    val weekCreatedTransformedDF = sinks
      .flatten
      .find(_.name == WeekCreatedSinkName).get.output

    assert(weekCreatedTransformedDF.count() == 5)
    assert(weekCreatedTransformedDF.columns.toSet === weekExpectedColumns)
    assert[String](weekCreatedTransformedDF, "week_id", "badfce75-de01-4cf3-8e48-a0d2799847ac")
    assert[Integer](weekCreatedTransformedDF, "week_number", 1)
    assert[Integer](weekCreatedTransformedDF, "week_status", 1)
    assert[Date](weekCreatedTransformedDF, "week_start_date", Date.valueOf("2020-12-27"))
    assert[Date](weekCreatedTransformedDF, "week_end_date", Date.valueOf("2021-01-28"))
    assert[String](weekCreatedTransformedDF, "week_term_id", "c58ddf53-caa8-46f5-8281-ded48bd19cf0")

  }

}
