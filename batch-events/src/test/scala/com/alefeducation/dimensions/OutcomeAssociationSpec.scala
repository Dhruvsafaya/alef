package com.alefeducation.dimensions

import java.util.TimeZone

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.util.Constants._
import org.scalatest.matchers.must.Matchers
import com.alefeducation.util.Helpers._
import com.alefeducation.util.DataFrameEqualityUtils._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.functions._


class OutcomeAssociationSpec extends SparkSuite with Matchers {

  trait Setup {
    implicit val transformer: OutcomeAssociationDimension = new OutcomeAssociationDimension(OutcomeAssociationDimensionName, spark)
  }

  test("Outcome association category attach/detach events") {
    val expParquetCategoryJson =
      """
        |[
        |{"categoryId":"e63d55bf-0f03-40e0-ac3f-55d7e024b18d","eventType":"OutcomeCategoryAttachedEvent","occurredOn":"2020-07-20 02:40:00.0","outcomeId":"strand-983141","eventdate":"2020-07-20"},
        |{"categoryId":"6637f9c8-c9d6-4f67-b4e1-74adba2c0deb","eventType":"OutcomeCategoryAttachedEvent","occurredOn":"2020-07-20 02:41:00.0","outcomeId":"strand-983142","eventdate":"2020-07-20"},
        |{"categoryId":"e63d55bf-0f03-40e0-ac3f-55d7e024b18d","eventType":"OutcomeCategoryDetachedEvent","occurredOn":"2020-07-20 02:42:00.0","outcomeId":"strand-983141","eventdate":"2020-07-20"}
        |]
      """.stripMargin
    val expRedshiftCategoryJson =
      """
        |[
        |{"outcome_association_updated_time":"2020-07-20T02:42:00.000Z","outcome_association_id":"e63d55bf-0f03-40e0-ac3f-55d7e024b18d","outcome_association_type":1,"outcome_association_status":4,"outcome_association_attach_status":1,"outcome_association_outcome_id":"strand-983141","outcome_association_created_time":"2020-07-20T02:40:00.000Z","outcome_association_dw_created_time":"2020-09-16T13:22:45.747Z", "outcome_association_dw_updated_time":null},
        |{"outcome_association_id":"6637f9c8-c9d6-4f67-b4e1-74adba2c0deb","outcome_association_type":1,"outcome_association_status":1,"outcome_association_attach_status":1,"outcome_association_outcome_id":"strand-983142","outcome_association_created_time":"2020-07-20T02:41:00.000Z","outcome_association_dw_created_time":"2020-09-16T13:22:45.747Z", "outcome_association_updated_time":null, "outcome_association_dw_updated_time":null},
        |{"outcome_association_id":"e63d55bf-0f03-40e0-ac3f-55d7e024b18d","outcome_association_type":1,"outcome_association_status":1,"outcome_association_attach_status":0,"outcome_association_outcome_id":"strand-983141","outcome_association_created_time":"2020-07-20T02:42:00.000Z","outcome_association_dw_created_time":"2020-09-16T13:22:45.747Z","outcome_association_updated_time":null, "outcome_association_dw_updated_time":null}
        |]
      """.stripMargin
    val expRedshiftCategoryDf = createDfFromJsonWithTimeCols(spark, expRedshiftCategoryJson)
    val expDeltaCategoryJson =
      """
        |[
        |{"outcome_association_updated_time":"2020-07-20T02:42:00.000Z","outcome_association_id":"e63d55bf-0f03-40e0-ac3f-55d7e024b18d","outcome_association_type":1,"outcome_association_status":4,"outcome_association_attach_status":1,"outcome_association_outcome_id":"strand-983141","outcome_association_created_time":"2020-07-20T02:40:00.000Z","outcome_association_dw_created_time":"2020-09-16T18:21:51.378Z","outcome_association_dw_updated_time":null},
        |{"outcome_association_id":"6637f9c8-c9d6-4f67-b4e1-74adba2c0deb","outcome_association_type":1,"outcome_association_status":1,"outcome_association_attach_status":1,"outcome_association_outcome_id":"strand-983142","outcome_association_created_time":"2020-07-20T02:41:00.000Z","outcome_association_dw_created_time":"2020-09-16T18:21:51.378Z","outcome_association_updated_time":null, "outcome_association_dw_updated_time":null},
        |{"outcome_association_id":"e63d55bf-0f03-40e0-ac3f-55d7e024b18d","outcome_association_type":1,"outcome_association_status":1,"outcome_association_attach_status":0,"outcome_association_outcome_id":"strand-983141","outcome_association_created_time":"2020-07-20T02:42:00.000Z","outcome_association_dw_created_time":"2020-09-16T18:21:51.378Z","outcome_association_updated_time":null, "outcome_association_dw_updated_time":null}
        |]
      """.stripMargin
    val expDeltaCategoryDf = createDfFromJsonWithTimeCols(spark, expDeltaCategoryJson)


    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetOutcomeCategorySource,
          value = """
                    |[
                    |{
                    |   "eventType":"OutcomeCategoryAttachedEvent",
                    |   "outcomeId":"strand-983141",
                    |   "categoryId":"e63d55bf-0f03-40e0-ac3f-55d7e024b18d",
                    |   "occurredOn":"2020-07-20 02:40:00.0"
                    |},
                    |{
                    |   "eventType":"OutcomeCategoryAttachedEvent",
                    |   "outcomeId":"strand-983142",
                    |   "categoryId":"6637f9c8-c9d6-4f67-b4e1-74adba2c0deb",
                    |   "occurredOn":"2020-07-20 02:41:00.0"
                    |},
                    |{
                    |   "eventType": "OutcomeCategoryDetachedEvent",
                    |   "outcomeId":"strand-983141",
                    |   "categoryId":"e63d55bf-0f03-40e0-ac3f-55d7e024b18d",
                    |   "occurredOn":"2020-07-20 02:42:00.0"
                    |}
                    |]
                    """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>

          val parquetCategoryDf = sinks.find(_.name === ParquetOutcomeCategorySink).get.output
          val redshiftCategoryDf = sinks.find(_.name === RedshiftOutcomeAssociationSink)
            .get.output.sort("outcome_association_created_time")
          val deltaCategoryDf = sinks.find(_.eventType === s"${OutcomeAssociationEntity}_$OutcomeAssociationCategoryType")
            .get.output.sort("outcome_association_created_time")

          assertSmallDatasetEquality(spark, OutcomeAssociationEntity, parquetCategoryDf, expParquetCategoryJson)
          assertSmallDatasetEquality(OutcomeAssociationEntity, redshiftCategoryDf, expRedshiftCategoryDf)
          assertSmallDatasetEquality(OutcomeAssociationEntity, deltaCategoryDf, expDeltaCategoryDf)
        }
      )
    }
  }

  test("Outcome association skill attach/detach events") {
    val expParquetSkillJson =
      """
        |[
        |{"skillId":"e63d55bf-0f03-40e0-ac3f-55d7e024b18d","eventType":"OutcomeSkillAttachedEvent","occurredOn":"2020-07-20 02:40:00.0","outcomeId":"strand-983141","eventdate":"2020-07-20"},
        |{"skillId":"6637f9c8-c9d6-4f67-b4e1-74adba2c0deb","eventType":"OutcomeSkillAttachedEvent","occurredOn":"2020-07-20 02:41:00.0","outcomeId":"strand-983142","eventdate":"2020-07-20"},
        |{"skillId":"e63d55bf-0f03-40e0-ac3f-55d7e024b18d","eventType":"OutcomeSkillDetachedEvent","occurredOn":"2020-07-20 02:42:00.0","outcomeId":"strand-983141","eventdate":"2020-07-20"}
        |]
      """.stripMargin
    val expRedshiftSkillJson =
      """
        |[
        |{"outcome_association_updated_time":"2020-07-20T02:42:00.000Z","outcome_association_id":"e63d55bf-0f03-40e0-ac3f-55d7e024b18d","outcome_association_type":2,"outcome_association_status":4,"outcome_association_attach_status":1,"outcome_association_outcome_id":"strand-983141","outcome_association_created_time":"2020-07-20T02:40:00.000Z","outcome_association_dw_created_time":"2020-09-16T13:22:45.747Z", "outcome_association_dw_updated_time":null},
        |{"outcome_association_id":"6637f9c8-c9d6-4f67-b4e1-74adba2c0deb","outcome_association_type":2,"outcome_association_status":1,"outcome_association_attach_status":1,"outcome_association_outcome_id":"strand-983142","outcome_association_created_time":"2020-07-20T02:41:00.000Z","outcome_association_dw_created_time":"2020-09-16T13:22:45.747Z", "outcome_association_updated_time":null, "outcome_association_dw_updated_time":null},
        |{"outcome_association_id":"e63d55bf-0f03-40e0-ac3f-55d7e024b18d","outcome_association_type":2,"outcome_association_status":1,"outcome_association_attach_status":0,"outcome_association_outcome_id":"strand-983141","outcome_association_created_time":"2020-07-20T02:42:00.000Z","outcome_association_dw_created_time":"2020-09-16T13:22:45.747Z","outcome_association_updated_time":null, "outcome_association_dw_updated_time":null}
        |]
      """.stripMargin
    val expRedshiftSkillDf = createDfFromJsonWithTimeCols(spark, expRedshiftSkillJson)
    val expDeltaSkillJson =
      """
        |[
        |{"outcome_association_updated_time":"2020-07-20T02:42:00.000Z","outcome_association_id":"e63d55bf-0f03-40e0-ac3f-55d7e024b18d","outcome_association_type":2,"outcome_association_status":4,"outcome_association_attach_status":1,"outcome_association_outcome_id":"strand-983141","outcome_association_created_time":"2020-07-20T02:40:00.000Z","outcome_association_dw_created_time":"2020-09-16T18:21:51.378Z","outcome_association_dw_updated_time":null},
        |{"outcome_association_id":"6637f9c8-c9d6-4f67-b4e1-74adba2c0deb","outcome_association_type":2,"outcome_association_status":1,"outcome_association_attach_status":1,"outcome_association_outcome_id":"strand-983142","outcome_association_created_time":"2020-07-20T02:41:00.000Z","outcome_association_dw_created_time":"2020-09-16T18:21:51.378Z","outcome_association_updated_time":null, "outcome_association_dw_updated_time":null},
        |{"outcome_association_id":"e63d55bf-0f03-40e0-ac3f-55d7e024b18d","outcome_association_type":2,"outcome_association_status":1,"outcome_association_attach_status":0,"outcome_association_outcome_id":"strand-983141","outcome_association_created_time":"2020-07-20T02:42:00.000Z","outcome_association_dw_created_time":"2020-09-16T18:21:51.378Z","outcome_association_updated_time":null, "outcome_association_dw_updated_time":null}
        |]
      """.stripMargin
    val expDeltaSkillDf = createDfFromJsonWithTimeCols(spark, expDeltaSkillJson)


    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetOutcomeSkillSource,
          value = """
                    |[
                    |{
                    |   "eventType":"OutcomeSkillAttachedEvent",
                    |   "outcomeId":"strand-983141",
                    |   "skillId":"e63d55bf-0f03-40e0-ac3f-55d7e024b18d",
                    |   "occurredOn":"2020-07-20 02:40:00.0"
                    |},
                    |{
                    |   "eventType":"OutcomeSkillAttachedEvent",
                    |   "outcomeId":"strand-983142",
                    |   "skillId":"6637f9c8-c9d6-4f67-b4e1-74adba2c0deb",
                    |   "occurredOn":"2020-07-20 02:41:00.0"
                    |},
                    |{
                    |   "eventType": "OutcomeSkillDetachedEvent",
                    |   "outcomeId":"strand-983141",
                    |   "skillId":"e63d55bf-0f03-40e0-ac3f-55d7e024b18d",
                    |   "occurredOn":"2020-07-20 02:42:00.0"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>

          val parquetSkillDf = sinks.find(_.name === ParquetOutcomeSkillSink).get.output
          val redshiftSkillDf = sinks.find(_.name === RedshiftOutcomeAssociationSink)
            .get.output.sort("outcome_association_created_time")
          val deltaSkillDf = sinks.find(_.eventType === s"${OutcomeAssociationEntity}_$OutcomeAssociationSkillType")
            .get.output.sort("outcome_association_created_time")

          assertSmallDatasetEquality(spark, OutcomeAssociationEntity, parquetSkillDf, expParquetSkillJson)
          assertSmallDatasetEquality(OutcomeAssociationEntity, redshiftSkillDf, expRedshiftSkillDf)
          assertSmallDatasetEquality(OutcomeAssociationEntity, deltaSkillDf, expDeltaSkillDf)
        }
      )
    }
  }

  def createDfFromJsonWithTimeCols(spark: SparkSession, json: String): DataFrame = {
    createDfFromJson(spark, json)
      .withColumn("outcome_association_created_time", col("outcome_association_created_time").cast(TimestampType))
      .withColumn("outcome_association_updated_time", col("outcome_association_updated_time").cast(TimestampType))
  }

}
