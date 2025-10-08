package com.alefeducation.dimensions

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.badge.transform.BadgeUpdatedTransform
import com.alefeducation.dimensions.badge.transform.BadgeUpdatedTransform.{BadgeUpdatedParquetSource, BadgeUpdatedTransformed, TenantRedshift}
import com.alefeducation.schema.tenant.Tenant
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock


class BadgeTransformTest extends SparkSuite {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  test("should construct badge dataframe from published") {
    val value =
      """
        |[
        |{
        |     "eventType": "BadgesMetaDataUpdatedEvent",
        |     "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013",
        |     "id": "8b8cf877-c6e8-4450-aaaf-2333a92abb3e",
        |	 "type": "WAVE",
        |    "order": 1,
        |    "title": "Wave Badge",
        |    "category": "LOGIN",
        |    "tenantCode": "moe",
        |    "finalDescription": "You have got every badge possible in this type.",
        |    "createdAt": "2023-04-18T12:19:37.503648",
        |    "k12Grades": [
        |      "5",
        |      "6",
        |      "7",
        |      "8"
        |    ],
        |    "rules": [
        |      {
        |        "tier": "BRONZE",
        |        "order": 1,
        |        "defaultThreshold": 5,
        |        "defaultCompletionMessage": "",
        |        "defaultDescription": "Continue to log in to earn a Bronze Badge.",
        |        "rulesByGrade": [
        |          {
        |            "k12Grade": "6",
        |            "threshold": 7,
        |            "description": "Login 7 times",
        |            "completionMessage": ""
        |          },
        |          {
        |            "k12Grade": "7",
        |            "threshold": 10,
        |            "description": "Login 10 times",
        |            "completionMessage": ""
        |          },
        |          {
        |            "k12Grade": "8",
        |            "threshold": 10,
        |            "description": "Login 10 times",
        |            "completionMessage": ""
        |          }
        |        ]
        |      },
        |      {
        |        "tier": "SILVER",
        |        "order": 2,
        |        "defaultThreshold": 10,
        |        "defaultCompletionMessage": "",
        |        "defaultDescription": "Continue to log in to earn a Silver Badge.",
        |        "rulesByGrade": [
        |          {
        |            "k12Grade": "6",
        |            "threshold": 15,
        |            "description": "Login 15 times to earn silver badge",
        |            "completionMessage": ""
        |          },
        |          {
        |            "k12Grade": "7",
        |            "threshold": 15,
        |            "description": "Login 15 times",
        |            "completionMessage": ""
        |          },
        |          {
        |            "k12Grade": "8",
        |            "threshold": 20,
        |            "description": "Login 20 times",
        |            "completionMessage": ""
        |          }
        |        ]
        |      },
        |      {
        |        "tier": "GOLD",
        |        "order": 3,
        |        "defaultThreshold": 20,
        |        "defaultDescription": "Continue to log in to earn a Gold Badge.",
        |        "rulesByGrade": [
        |          {
        |            "k12Grade": "6",
        |            "threshold": 25,
        |            "description": "Login 25 times to get platinum badge"
        |          },
        |          {
        |            "k12Grade": "7",
        |            "threshold": 25,
        |            "description": "Login 25 times"
        |          },
        |          {
        |            "k12Grade": "8",
        |            "threshold": 30,
        |            "description": "Login 30 times"
        |          }
        |        ]
        |      }
        |    ],
        |    "occurredOn": "2023-05-02 09:55:22"
        |}
        |]
        """.stripMargin

    val tenantValues =
      """
        |[
        |  {
        |    "tenant_dw_id": 2,
        |    "tenant_id": "93e4949d-7eff-4707-9201-dac917a5e013",
        |    "tenant_name": "MOE",
        |    "tenant_timezone": "Asia/Dubai"
        |  }
        |]
        |""".stripMargin

    val expectedColumns = Set(
      "bdg_id",
      "bdg_tier",
      "bdg_grade",
      "bdg_type",
      "bdg_tenant_dw_id",
      "bdg_tenant_id",
      "bdg_title",
      "bdg_category",
      "bdg_threshold",
      "bdg_dw_created_time",
      "bdg_dw_updated_time",
      "bdg_created_time",
      "bdg_status",
      "bdg_deleted_time",
      "bdg_updated_time",
      "bdg_active_until"
    )

    val sprk = spark
    import sprk.implicits._

    val inputDF = spark.read.json(Seq(value).toDS())
    when(service.readOptional(BadgeUpdatedParquetSource, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(inputDF))

    val tenantDF = spark.read.json(Seq(tenantValues).toDS())
    when(service.readFromRedshiftOptional[Tenant](TenantRedshift, selectedCols = List("tenant_dw_id", "tenant_id", "tenant_name", "tenant_timezone"))).thenReturn(Some(tenantDF))

    val transformer = new BadgeUpdatedTransform(sprk, service)
    val sinks = transformer.transform()

    val df = sinks.filter(_.get.name == BadgeUpdatedTransformed).head.get.output

    assert(df.columns.toSet === expectedColumns)
    // workaround to overcome wrong ColumnPruning on .count()
    assert(df.collect().length == 12)

    val testDf = df.filter(col("bdg_id")==="8b8cf877-c6e8-4450-aaaf-2333a92abb3e"
      && col("bdg_grade")=== 6
      && col("bdg_tier")==="BRONZE"
    ).toDF()

    assert[String](testDf, "bdg_id", "8b8cf877-c6e8-4450-aaaf-2333a92abb3e")
    assert[String](testDf, "bdg_tier", "BRONZE")
    assert[Int](testDf, "bdg_grade", 6)
    assert[String](testDf, "bdg_type", "WAVE")
    assert[Int](testDf, "bdg_tenant_dw_id", 2)
    assert[String](testDf, "bdg_tenant_id", "93e4949d-7eff-4707-9201-dac917a5e013")
    assert[String](testDf, "bdg_title", "Wave Badge")
    assert[String](testDf, "bdg_category", "LOGIN")
    assert[Int](testDf, "bdg_threshold", 7)
    assert[String](testDf, "bdg_dw_updated_time", null)
    assert[String](testDf, "bdg_created_time", "2023-05-02 09:55:22.0")
    assert[Int](testDf, "bdg_status", 1)
    assert[String](testDf, "bdg_deleted_time", null)
    assert[String](testDf, "bdg_updated_time", null)
    assert[String](testDf, "bdg_active_until", null)
  }
}
