package com.alefeducation.dimensions.generic_transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.BaseDimensionSpec
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class GenericFactTransformSpec extends SparkSuite with BaseDimensionSpec {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  test("should construct transformation data frame for fact jobs with simple mapping inside configuration") {
    val updatedValue =
      """
        |[
        |{
        |   "eventType":"ItemPurchasedEvent",
        |	"tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
        |   "purchaseId": "8b8cf877-c6e8-4450-aaaf-2333a92abb3e",
        |   "itemType": "AVATAR",
        |   "itemId": "9184acd2-02f7-49e5-9f4e-f0c034188e6b",
        |   "itemTitle": "test.png",
        |   "itemDescription": "test description",
        |   "studentId": "2b8cf877-c6e8-4450-aaaf-2333a92abb3e",
        |   "gradeId": "3b8cf877-c6e8-4450-aaaf-2333a92abb3e",
        |   "sectionId": "4b8cf877-c6e8-4450-aaaf-2333a92abb3e",
        |   "schoolId": "7b8cf877-c6e8-4450-aaaf-2333a92abb3e",
        |   "transactionId": "8b8cf877-c6e8-4450-aaaf-2333a92abb3e",
        |   "academicYearId": "9b8cf877-c6e8-4450-aaaf-2333a92abb3e",
        |   "academicYear": "2024",
        |   "redeemedStars": 10,
        |   "eventId": "3d7ac395-cfe6-4ad6-8ac7-f792f7d2cb0c",
        |	"occurredOn": "2021-06-23 05:33:25.921"
        |}
        |]
        |""".stripMargin

    val expectedColumns = Set(
      "fip_dw_id",
      "fip_item_id",
      "fip_item_type",
      "fip_item_title",
      "fip_item_description",
      "fip_transaction_id",
      "fip_school_id",
      "fip_grade_id",
      "fip_section_id",
      "fip_academic_year_id",
      "fip_academic_year",
      "fip_student_id",
      "fip_tenant_id",
      "fip_redeemed_stars",
      "fip_created_time",
      "fip_dw_created_time",
      "fip_id",
      "fip_date_dw_id",
      "eventdate"
    )

    val sprk = spark
    import sprk.implicits._

    val transformer = new GenericFactTransform(sprk, service, "marketplace-purchase-transform")

    val inputDf = spark.read.json(Seq(updatedValue).toDS())
    when(service.readOptional("parquet-marketplace-purchase-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(inputDf))
    when(service.getStartIdUpdateStatus("fact_item_purchase")).thenReturn(1001)

    val sinks = transformer.transform()

    val df = sinks.head.output

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 1)
    assert[String](df, "fip_id", "8b8cf877-c6e8-4450-aaaf-2333a92abb3e")
    assert[String](df, "fip_tenant_id", "93e4949d-7eff-4707-9201-dac917a5e013")
    assert[String](df, "fip_school_id", "7b8cf877-c6e8-4450-aaaf-2333a92abb3e")
    assert[String](df, "fip_student_id", "2b8cf877-c6e8-4450-aaaf-2333a92abb3e")
    assert[String](df, "fip_transaction_id", "8b8cf877-c6e8-4450-aaaf-2333a92abb3e")
    assert[String](df, "fip_created_time", "2021-06-23 05:33:25.921")
    assert[Int](df, "fip_redeemed_stars", 10)
    assert[String](df, "fip_item_type", "AVATAR")
    assert[String](df, "fip_academic_year", "2024")
    assert[String](df, "fip_academic_year_id", "9b8cf877-c6e8-4450-aaaf-2333a92abb3e")
    assert[String](df, "fip_item_id", "9184acd2-02f7-49e5-9f4e-f0c034188e6b")
    assert[String](df, "fip_item_title", "test.png")
    assert[String](df, "fip_item_description", "test description")
    assert[String](df, "fip_grade_id", "3b8cf877-c6e8-4450-aaaf-2333a92abb3e")
    assert[String](df, "fip_section_id", "4b8cf877-c6e8-4450-aaaf-2333a92abb3e")
    assert[Int](df, "fip_dw_id", 1001)
    assert[String](df, "fip_date_dw_id", "20210623")
    assert[String](df, "eventdate", "2021-06-23")
  }

  test("should construct transformation data frame for PurchaseTransactionEvent") {
    val updatedValue =
      """
        |[
        |{
        |   "eventType":"PurchaseTransactionEvent",
        |	"tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
        |   "availableStars": 100,
        |   "itemCost": 10,
        |   "starBalance": 90,
        |   "itemId": "9184acd2-02f7-49e5-9f4e-f0c034188e6b",
        |   "itemType": "AVATAR",
        |   "gradeId": "3b8cf877-c6e8-4450-aaaf-2333a92abb3e",
        |   "sectionId": "4b8cf877-c6e8-4450-aaaf-2333a92abb3e",
        |   "schoolId": "7b8cf877-c6e8-4450-aaaf-2333a92abb3e",
        |   "transactionId": "8b8cf877-c6e8-4450-aaaf-2333a92abb3e",
        |   "academicYearId": "9b8cf877-c6e8-4450-aaaf-2333a92abb3e",
        |   "academicYear": "2024",
        |   "studentId": "2b8cf877-c6e8-4450-aaaf-2333a92abb3e",
        |   "eventId": "3d7ac395-cfe6-4ad6-8ac7-f792f7d2cb0c",
        |	"occurredOn": "2021-06-23 05:33:25.921"
        |}
        |]
        |""".stripMargin

    val expectedColumns = Set(
      "fit_dw_id",
      "fit_item_id",
      "fit_item_type",
      "fit_school_id",
      "fit_grade_id",
      "fit_section_id",
      "fit_academic_year_id",
      "fit_academic_year",
      "fit_student_id",
      "fit_tenant_id",
      "fit_created_time",
      "fit_dw_created_time",
      "fit_id",
      "fit_date_dw_id",
      "eventdate",
      "fit_available_stars",
      "fit_item_cost",
      "fit_star_balance"
    )

    val sprk = spark
    import sprk.implicits._

    val transformer = new GenericFactTransform(sprk, service, "marketplace-transaction-transform")

    val inputDf = spark.read.json(Seq(updatedValue).toDS())
    when(service.readOptional("parquet-marketplace-transaction-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(inputDf))
    when(service.getStartIdUpdateStatus("fact_item_transaction")).thenReturn(1001)

    val sinks = transformer.transform()

    val df = sinks.head.output

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 1)
    assert[String](df, "fit_id", "8b8cf877-c6e8-4450-aaaf-2333a92abb3e")
    assert[String](df, "fit_tenant_id", "93e4949d-7eff-4707-9201-dac917a5e013")
    assert[String](df, "fit_school_id", "7b8cf877-c6e8-4450-aaaf-2333a92abb3e")
    assert[String](df, "fit_student_id", "2b8cf877-c6e8-4450-aaaf-2333a92abb3e")
    assert[String](df, "fit_created_time", "2021-06-23 05:33:25.921")
    assert[Int](df, "fit_available_stars", 100)
    assert[Int](df, "fit_item_cost", 10)
    assert[Int](df, "fit_star_balance", 90)
    assert[String](df, "fit_item_type", "AVATAR")
    assert[String](df, "fit_academic_year", "2024")
    assert[String](df, "fit_academic_year_id", "9b8cf877-c6e8-4450-aaaf-2333a92abb3e")
    assert[String](df, "fit_item_id", "9184acd2-02f7-49e5-9f4e-f0c034188e6b")
    assert[String](df, "fit_grade_id", "3b8cf877-c6e8-4450-aaaf-2333a92abb3e")
    assert[String](df, "fit_section_id", "4b8cf877-c6e8-4450-aaaf-2333a92abb3e")
    assert[Int](df, "fit_dw_id", 1001)
    assert[String](df, "fit_date_dw_id", "20210623")
    assert[String](df, "eventdate", "2021-06-23")
  }
}
