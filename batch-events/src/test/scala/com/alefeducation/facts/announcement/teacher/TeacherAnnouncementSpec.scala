package com.alefeducation.facts.announcement.teacher

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.facts.announcement.AnnouncementUtils._
import com.alefeducation.facts.announcement.AnnouncementUtilsTest.{assertCommonFields, expectedColumns}
import com.alefeducation.facts.announcement.teacher.TeacherAnnouncementTransform._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{ArrayType, StringType}
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class TeacherAnnouncementSpec extends SparkSuite {

  val sprk: SparkSession = spark
  val service = mock[SparkBatchService]

  test("transform teacher announcement event for classes successfully") {
    val value = """
                  |[
                  |{
                  |  "eventType": "TeacherAnnouncementSentEvent",
                  |  "announcementId": "65cf3dd8-b819-446d-b0bf-b7216b7bbba9",
                  |	 "teacherId": "c3bed03b-19ff-4718-b6bf-17a359a349cd",
                  |  "classIds": [
                  |     "8a93e5d0-cfa4-46da-b52f-1202eabaf213",
                  |     "02272ee2-3b5b-425a-b2e3-a7c4f5f3a78d",
                  |     "e1c0dcb7-c3ad-4f64-b28a-d2bc29d5d84f"
                  |  ],
                  |	 "studentIds": null,
                  |	 "hasAttachment": false,
                  |  "announcementType": "GUARDIANS",
                  |	 "occurredOn": "2022-11-17 09:29:24.430",
                  |  "eventDateDw": 20221117,
                  |  "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
                  |}]
        """.stripMargin
    val sprk = spark
    import sprk.implicits._

    val transformer = new TeacherAnnouncementTransform(sprk, service)

    val inputDF = spark.read.json(Seq(value).toDS())
      .withColumn(StudentIdsColName, lit(null).cast(ArrayType(StringType)))
    when(service.readOptional(ParquetTeacherAnnouncementSource, sprk)).thenReturn(Some(inputDF))
    val df = transformer.transform().get.output

    assert(df.columns.toSet === expectedColumns)
    assert[String](df, "fa_recipient_type_description", ClassTypeDesc)
    assert[Int](df, "fa_recipient_type", ClassTypeId)
    assertCommonFields(TeacherRoleName, ActiveStatus, df)

    assertSet[String](
      df,
      "fa_recipient_id",
      Set("8a93e5d0-cfa4-46da-b52f-1202eabaf213",
          "02272ee2-3b5b-425a-b2e3-a7c4f5f3a78d",
          "e1c0dcb7-c3ad-4f64-b28a-d2bc29d5d84f")
    )
  }

  test("transform teacher announcement event for students successfully") {
    val value = """
                  |[
                  |{
                  |  "eventType": "TeacherAnnouncementSentEvent",
                  |  "announcementId": "65cf3dd8-b819-446d-b0bf-b7216b7bbba9",
                  |	 "teacherId": "c3bed03b-19ff-4718-b6bf-17a359a349cd",
                  |  "classIds": null,
                  |	 "studentIds": [
                  |     "74a9f731-4545-4fcf-8f9b-2aea397f69f1",
                  |     "758a61c9-f7a1-416a-b003-ab15e381a3d7"
                  |  ],
                  |	 "hasAttachment": false,
                  |  "announcementType": "GUARDIANS",
                  |	 "occurredOn": "2022-11-17 09:29:24.430",
                  |  "eventDateDw": 20221117,
                  |  "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
                  |}]
        """.stripMargin
    val sprk = spark
    import sprk.implicits._

    val transformer = new TeacherAnnouncementTransform(sprk, service)

    val inputDF = spark.read.json(Seq(value).toDS())
      .withColumn(ClassIdsColName, lit(null).cast(ArrayType(StringType)))
    when(service.readOptional(ParquetTeacherAnnouncementSource, sprk)).thenReturn(Some(inputDF))
    val df = transformer.transform().get.output

    assert(df.columns.toSet === expectedColumns)
    assert[String](df, "fa_recipient_type_description", StudentTypeDesc)
    assert[Int](df, "fa_recipient_type", StudentTypeId)
    assertCommonFields(TeacherRoleName, ActiveStatus, df)

    assertSet[String](
      df,
      "fa_recipient_id",
      Set("74a9f731-4545-4fcf-8f9b-2aea397f69f1",
          "758a61c9-f7a1-416a-b003-ab15e381a3d7")
    )
  }
}
