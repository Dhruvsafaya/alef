package com.alefeducation.dimensions.organization

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.organization.OrganizationTransform.OrganizationMutatedParquetSource
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class OrganizationTransformTest extends SparkSuite {
  val sprk: SparkSession = spark
  val service = mock[SparkBatchService]

  val expectedColumns = List(
    "organization_id",
    "organization_name",
    "organization_country",
    "organization_code",
    "organization_status",
    "organization_tenant_code",
    "organization_created_by",
    "organization_updated_by",
    "organization_created_time",
    "organization_dw_created_time",
    "organization_dw_updated_time",
    "organization_updated_time",
    "organization_deleted_time"
  ).toSet


  test("transform organization created event successfully") {

    val value =
      """
        |[
        |{
        |  "eventType": "OrganizationCreatedEvent",
        |  "id": "93ef3952-e606-4be2-8a08-35b692a67219",
        |  "name": "testorg2sep21",
        |  "country":  "AF",
        |  "code": "testorg2sep21",
        |  "tenantCode": "sharks",
        |  "isActive": "true",
        |  "createdAt": "2022-09-21T13:08:52.286537831",
        |	 "updatedAt": "2022-09-21T13:08:52.286537831",
        |	 "createdBy": "99ef3952-e606-4be2-8a08-35b692a67219",
        |	 "updatedBy": "99ef3952-e606-4be2-8a08-35b692a67219",
        |  "occurredOn": "2021-06-23 05:33:24.921"
        |}]
        """.stripMargin

    val sprk = spark
    import sprk.implicits._
    val transformer = new OrganizationTransform(sprk, service)
    val inputDF = spark.read.json(Seq(value).toDS())
    when(service.readOptional(OrganizationMutatedParquetSource, sprk)).thenReturn(Some(inputDF))

    val df = transformer.transform().head.get.output
    assert(df)
  }

  test("transform organization updated event successfully") {

    val value =
      """
        |[
        |{
        |  "eventType": "OrganizationUpdatedEvent",
        |  "id": "93ef3952-e606-4be2-8a08-35b692a67219",
        |  "name": "testorg2sep21",
        |  "country":  "AF",
        |  "code": "testorg2sep21",
        |  "tenantCode": "sharks",
        |  "isActive": "true",
        |  "createdAt": "2022-09-21T13:08:52.286537831",
        |	 "updatedAt": "2022-09-21T13:08:52.286537832",
        |	 "createdBy": "99ef3952-e606-4be2-8a08-35b692a67219",
        |	 "updatedBy": "99ef3952-e606-4be2-8a08-35b692a67219",
        |  "occurredOn": "2021-06-23 05:33:24.921"
        |}]
        """.stripMargin
    val sprk = spark
    import sprk.implicits._
    val transformer = new OrganizationTransform(sprk, service)
    val inputDF = spark.read.json(Seq(value).toDS())
    when(service.readOptional(OrganizationMutatedParquetSource, sprk)).thenReturn(Some(inputDF))
    val df = transformer.transform().tail.head.get.output
    assert(df)
  }

  private def assert(df: DataFrame): Unit = {
    assert(df.columns.toSet === expectedColumns)
    assert[String](df, "organization_id", "93ef3952-e606-4be2-8a08-35b692a67219")
    assert[String](df, "organization_name", "testorg2sep21")
    assert[String](df, "organization_country", "AF")
    assert[String](df, "organization_code", "testorg2sep21")
    assert[Int](df, "organization_status", 1)
    assert[String](df, "organization_tenant_code", "sharks")
    assert[String](df, "organization_created_by", "99ef3952-e606-4be2-8a08-35b692a67219")
    assert[String](df, "organization_updated_by", "99ef3952-e606-4be2-8a08-35b692a67219")
    assert[String](df, "organization_created_time", "2021-06-23 05:33:24.921")
  }
}
