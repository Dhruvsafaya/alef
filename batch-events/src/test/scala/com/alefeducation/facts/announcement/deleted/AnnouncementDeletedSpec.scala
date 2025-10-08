package com.alefeducation.facts.announcement.deleted

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.facts.announcement.AnnouncementUtils._
import com.alefeducation.facts.announcement.AnnouncementUtilsTest.{assertCommonFields, expectedColumns}
import com.alefeducation.facts.announcement.deleted.AnnouncementDeletedTransform.ParquetAnnouncementDeletedSource
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class AnnouncementDeletedSpec extends SparkSuite {

  val sprk: SparkSession = spark
  val service = mock[SparkBatchService]

  test("transform announcement deleted events successfully") {
    val value =
      """
        |[
        |{
        |  "eventType": "AnnouncementDeletedEvent",
        |  "announcementId": "65cf3dd8-b819-446d-b0bf-b7216b7bbba9",
        |  "occurredOn": "2022-11-17 09:29:24.430",
        |  "eventDateDw": 20221117,
        |  "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
        |}
        |]
        """.stripMargin

    val deltaValue =
      """
        |[
        |{
        |   "fa_tenant_id": "93e4949d-7eff-4707-9201-dac917a5e013",
        |   "fa_id": "65cf3dd8-b819-446d-b0bf-b7216b7bbba9",
        |   "fa_created_time": "2022-11-17 08:28:24.430",
        |   "fa_dw_created_time": "2022-11-17 08:30:24.430",
        |   "fa_status": 1,
        |   "fa_admin_id": "c3bed03b-19ff-4718-b6bf-17a359a349cd",
        |   "fa_role_id": "TEACHER",
        |   "fa_recipient_type": 3,
        |   "fa_recipient_type_description": "CLASS",
        |   "fa_recipient_id": "8a93e5d0-cfa4-46da-b52f-1202eabaf213",
        |   "fa_type": 2,
        |   "fa_has_attachment": false
        |},
        |{
        |   "fa_tenant_id": "93e4949d-7eff-4707-9201-dac917a5e013",
        |   "fa_id": "65cf3dd8-b819-446d-b0bf-b7216b7bbba9",
        |   "fa_created_time": "2022-11-17 08:28:24.430",
        |   "fa_dw_created_time": "2022-11-17 08:30:24.430",
        |   "fa_status": 1,
        |   "fa_admin_id": "c3bed03b-19ff-4718-b6bf-17a359a349cd",
        |   "fa_role_id": "TEACHER",
        |   "fa_recipient_type": 3,
        |   "fa_recipient_type_description": "CLASS",
        |   "fa_recipient_id": "02272ee2-3b5b-425a-b2e3-a7c4f5f3a78d",
        |   "fa_type": 2,
        |   "fa_has_attachment": false
        |}
        |]
        |""".stripMargin
    val sprk = spark
    import sprk.implicits._

    val transformer = new AnnouncementDeletedTransform(sprk, service)

    val inputDF = spark.read.json(Seq(value).toDS())
    when(
      service.readUniqueOptional(ParquetAnnouncementDeletedSource, sprk, uniqueColNames=Seq("announcementId"))
    ).thenReturn(Some(inputDF))
    val inputDeltaDF = spark.read.json(Seq(deltaValue).toDS())
    when(service.readOptional(DeltaAnnouncementSource, sprk)).thenReturn(Some(inputDeltaDF))

    val df = transformer.transform().get.output

    assert(df.columns.toSet === expectedColumns)
    assert[String](df, "fa_recipient_type_description", ClassTypeDesc)
    assert[Int](df, "fa_recipient_type", ClassTypeId)
    assertCommonFields(TeacherRoleName, DeletedStatus, df)

    assertSet[String](
      df,
      "fa_recipient_id",
      Set("8a93e5d0-cfa4-46da-b52f-1202eabaf213",
          "02272ee2-3b5b-425a-b2e3-a7c4f5f3a78d")
    )
  }
}
