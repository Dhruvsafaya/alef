package com.alefeducation.dimensions

import java.util.TimeZone

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.util.DataFrameEqualityUtils.{assertSmallDatasetEquality, createDfFromJson}
import com.alefeducation.util.Helpers._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.matchers.must.Matchers

class CCLActivityAssociationDimensionSpec extends SparkSuite with Matchers {

  trait Setup {
    implicit val transformer: CCLActivityAssociationDimension = new CCLActivityAssociationDimension(spark)
  }

  test("in Activity Outcome attached") {
    val expParquetJson =
      """
        |[
        |{"activityUuid":"581b3b29-d74e-45c0-ba7a-000000009901","eventType":"ActivityOutcomeAttachedEvent","lessonId":9901,"occurredOn":"2021-06-16 14:11:08.786","outcomeId":"strand-289","eventdate":"2021-06-16"}
        |]
      """.stripMargin
    val expRedshiftJson =
      """
        |[
        |{"lo_association_status":1,"lo_association_id":"strand-289","lo_association_type":2,"lo_association_lo_id":"581b3b29-d74e-45c0-ba7a-000000009901","lo_association_attach_status":1,"lo_association_lo_ccl_id":9901,"lo_association_created_time":"2021-06-16T14:11:08.786Z","lo_association_dw_created_time":"2021-06-29T13:06:16.112Z","lo_association_updated_time":null,"lo_association_dw_updated_time":null}
        |]
      """.stripMargin
    val expRedshiftDf = createDfFromJsonWithTimeCols(spark, expRedshiftJson)
    val expDeltaJson =
      """
        |[
        |{"lo_association_status":1,"lo_association_id":"strand-289","lo_association_type":2,"lo_association_lo_id":"581b3b29-d74e-45c0-ba7a-000000009901","lo_association_attach_status":1,"lo_association_lo_ccl_id":9901,"lo_association_created_time":"2021-06-16T14:11:08.786Z","lo_association_dw_created_time":"2021-06-29T13:06:16.112Z","lo_association_updated_time":null,"lo_association_dw_updated_time":null}
        |]
      """.stripMargin
    val expDeltaDf = createDfFromJsonWithTimeCols(spark, expDeltaJson)

    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetCCLActivityOutcomeSource,
          value = """
                    |{
                    |  "eventType": "ActivityOutcomeAttachedEvent",
                    |  "lessonId":9901,
                    |  "activityUuid":"581b3b29-d74e-45c0-ba7a-000000009901",
                    |  "outcomeId":"strand-289",
                    |  "occurredOn":"2021-06-16 14:11:08.786"
                    |}
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>

          val parquet = sinks.find(_.name === ParquetCCLActivityOutcomeSink).get.output
          val redshift = sinks.find(_.name === RedshiftLearningObjectiveAssociationDimSink)
            .get.output
          val delta = sinks.find(_.name === DeltaCCLActivityAssociationSink)
            .get.output

          assertSmallDatasetEquality(spark, LearningObjectiveAssociationEntity, parquet, expParquetJson)
          assertSmallDatasetEquality(LearningObjectiveAssociationEntity, redshift, expRedshiftDf)
          assertSmallDatasetEquality(LearningObjectiveAssociationEntity, delta, expDeltaDf)
        }
      )
    }
  }

  test("in Activity Outcome detached") {
    val expParquetJson =
      """
        |[
        |{"activityUuid":"581b3b29-d74e-45c0-ba7a-000000009901","eventType":"ActivityOutcomeDetachedEvent","lessonId":9901,"occurredOn":"2021-06-16 14:11:08.763","outcomeId":"strand-24","eventdate":"2021-06-16"}
        |]
      """.stripMargin
    val expRedshiftJson =
      """
        |[
        |{"lo_association_status":1,"lo_association_id":"strand-24","lo_association_type":2,"lo_association_lo_id":"581b3b29-d74e-45c0-ba7a-000000009901","lo_association_attach_status":0,"lo_association_lo_ccl_id":9901,"lo_association_created_time":"2021-06-16T14:11:08.763Z","lo_association_dw_created_time":"2021-06-30T09:11:48.408Z","lo_association_updated_time":null,"lo_association_dw_updated_time":null}
        |]
      """.stripMargin
    val expRedshiftDf = createDfFromJsonWithTimeCols(spark, expRedshiftJson)
    val expDeltaJson =
      """
        |[
        |{"lo_association_status":1,"lo_association_id":"strand-24","lo_association_type":2,"lo_association_lo_id":"581b3b29-d74e-45c0-ba7a-000000009901","lo_association_attach_status":0,"lo_association_lo_ccl_id":9901,"lo_association_created_time":"2021-06-16T14:11:08.763Z","lo_association_dw_created_time":"2021-06-30T09:12:58.717Z","lo_association_updated_time":null,"lo_association_dw_updated_time":null}
        |]
      """.stripMargin
    val expDeltaDf = createDfFromJsonWithTimeCols(spark, expDeltaJson)

    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetCCLActivityOutcomeSource,
          value = """
                    |{
                    |  "eventType": "ActivityOutcomeDetachedEvent",
                    |  "lessonId":9901,
                    |  "activityUuid":"581b3b29-d74e-45c0-ba7a-000000009901",
                    |  "outcomeId":"strand-24",
                    |  "occurredOn":"2021-06-16 14:11:08.763"
                    |}
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>

          val parquet = sinks.find(_.name === ParquetCCLActivityOutcomeSink).get.output
          val redshift = sinks.find(_.name === RedshiftLearningObjectiveAssociationDimSink)
            .get.output
          val delta = sinks.find(_.name === DeltaCCLActivityAssociationSink)
            .get.output

          assertSmallDatasetEquality(spark, LearningObjectiveAssociationEntity, parquet, expParquetJson)
          assertSmallDatasetEquality(LearningObjectiveAssociationEntity, redshift, expRedshiftDf)
          assertSmallDatasetEquality(LearningObjectiveAssociationEntity, delta, expDeltaDf)
        }
      )
    }
  }

  def createDfFromJsonWithTimeCols(spark: SparkSession, json: String): DataFrame = {
    createDfFromJson(spark, json)
      .withColumn(s"${LearningObjectiveAssociationEntity}_created_time", col(s"${LearningObjectiveAssociationEntity}_created_time").cast(TimestampType))
      .withColumn(s"${LearningObjectiveAssociationEntity}_updated_time", col(s"${LearningObjectiveAssociationEntity}_updated_time").cast(TimestampType))
  }
}
