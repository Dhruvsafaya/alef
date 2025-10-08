package com.alefeducation.facts.announcement.superintendent

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.facts.announcement.AnnouncementUtils._
import com.alefeducation.facts.announcement.AnnouncementUtilsTest.{assertCommonFields, expectedColumns}
import com.alefeducation.facts.announcement.superintendent.SuperintendentAnnouncementTransform.ParquetSuperintendentAnnouncementSource
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{ArrayType, StringType}
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class SuperintendentAnnouncementSpec extends SparkSuite {

  val sprk: SparkSession = spark
  val service = mock[SparkBatchService]

  test("transform superintendent announcement event for grades successfully") {
    val value =
      """
        |[
        |{
        |  "eventType": "SuperintendentAnnouncementSentEvent",
        |  "announcementId": "65cf3dd8-b819-446d-b0bf-b7216b7bbba9",
        |  "superintendentId": "c3bed03b-19ff-4718-b6bf-17a359a349cd",
        |  "schoolIds": null,
        |  "gradeIds": [
        |     "74a9f731-4545-4fcf-8f9b-2aea397f69f1",
        |     "758a61c9-f7a1-416a-b003-ab15e381a3d7"
        |  ],
        |  "hasAttachment": false,
        |  "announcementType": "GUARDIANS",
        |  "occurredOn": "2022-11-17 09:29:24.430",
        |  "eventDateDw": 20221117,
        |  "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
        |}
        |]
        """.stripMargin
    val sprk = spark
    import sprk.implicits._

    val transformer = new SuperintendentAnnouncementTransform(sprk, service)

    val inputDF = spark.read.json(Seq(value).toDS())
      .withColumn(SchoolIdsColName, lit(null).cast(ArrayType(StringType)))
    when(service.readOptional(ParquetSuperintendentAnnouncementSource, sprk)).thenReturn(Some(inputDF))
    val df = transformer.transform().get.output

    assert(df.columns.toSet === expectedColumns)
    assert[String](df, "fa_recipient_type_description", GradeTypeDesc)
    assert[Int](df, "fa_recipient_type", GradeTypeId)
    assertCommonFields(SuperintendentRoleName, ActiveStatus, df)

    assertSet[String](
      df,
      "fa_recipient_id",
      Set("74a9f731-4545-4fcf-8f9b-2aea397f69f1",
          "758a61c9-f7a1-416a-b003-ab15e381a3d7")
    )
  }

  test("transform superintendent announcement event for schools successfully") {
    val value =
      """
        |[
        |{
        |  "eventType": "SuperintendentAnnouncementSentEvent",
        |  "announcementId": "65cf3dd8-b819-446d-b0bf-b7216b7bbba9",
        |  "superintendentId": "c3bed03b-19ff-4718-b6bf-17a359a349cd",
        |  "schoolIds": [
        |     "0744486a-d0c1-4482-80db-a0a839327b0f",
        |     "58cdcc3f-39a4-41ed-b851-d8f1254d20af"
        |  ],
        |  "gradeIds": null,
        |  "hasAttachment": false,
        |  "announcementType": "GUARDIANS",
        |  "occurredOn": "2022-11-17 09:29:24.430",
        |  "eventDateDw": 20221117,
        |  "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
        |}
        |]
        """.stripMargin
    val sprk = spark
    import sprk.implicits._

    val transformer = new SuperintendentAnnouncementTransform(sprk, service)

    val inputDF = spark.read.json(Seq(value).toDS())
      .withColumn(GradeIdsColName, lit(null).cast(ArrayType(StringType)))
    when(service.readOptional(ParquetSuperintendentAnnouncementSource, sprk)).thenReturn(Some(inputDF))
    val df = transformer.transform().get.output

    assert(df.columns.toSet === expectedColumns)
    assert[String](df, "fa_recipient_type_description", SchoolTypeDesc)
    assert[Int](df, "fa_recipient_type", SchoolTypeId)
    assertCommonFields(SuperintendentRoleName, ActiveStatus, df)

    assertSet[String](
      df,
      "fa_recipient_id",
      Set("0744486a-d0c1-4482-80db-a0a839327b0f",
          "58cdcc3f-39a4-41ed-b851-d8f1254d20af")
    )
  }

}
