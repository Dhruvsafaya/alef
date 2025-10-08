package com.alefeducation.facts.announcement.principal

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.facts.announcement.AnnouncementUtils._
import com.alefeducation.facts.announcement.AnnouncementUtilsTest.{assertCommonFields, expectedColumns}
import com.alefeducation.facts.announcement.principal.PrincipalAnnouncementTransform.ParquetPrincipalAnnouncementSource
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{ArrayType, StringType}
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class PrincipalAnnouncementSpec extends SparkSuite {

  val sprk: SparkSession = spark
  val service = mock[SparkBatchService]

  test("transform principal announcement event successfully") {
    val value =
      """
        |[
        |{
        |  "eventType": "PrincipalAnnouncementSentEvent",
        |  "announcementId": "65cf3dd8-b819-446d-b0bf-b7216b7bbba9",
        |  "principalId": "c3bed03b-19ff-4718-b6bf-17a359a349cd",
        |  "schoolId": "74a9f731-4545-4fcf-8f9b-2aea397f69f1",
        |  "gradeIds": ["grade_uuid1", "grade_uuid2"],
        |  "classIds": null,
        |  "studentIds": null,
        |  "hasAttachment": false,
        |  "announcementType": "GUARDIANS",
        |  "occurredOn": "2022-11-17 09:29:24.430",
        |  "eventDateDw": 20221117,
        |  "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
        |},
        |{
        |  "eventType": "PrincipalAnnouncementSentEvent",
        |  "announcementId": "77777777-b819-446d-b0bf-b7216b7bbba9",
        |  "principalId": "c3bed03b-19ff-4718-b6bf-17a359a349cd",
        |  "schoolId": "74a9f731-4545-4fcf-8f9b-2aea397f69f1",
        |  "gradeIds": null,
        |  "classIds": null,
        |  "studentIds": ["student_uuid1"],
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

    val transformer = new PrincipalAnnouncementTransform(sprk, service)
    val inputDF = spark.read.json(Seq(value).toDS())
      .withColumn(ClassIdsColName, lit(null).cast(ArrayType(StringType)))
      .withColumn(StudentIdsColName, col(StudentIdsColName).cast(ArrayType(StringType)))
    when(service.readOptional(ParquetPrincipalAnnouncementSource, sprk)).thenReturn(Some(inputDF))

    val df = transformer.transform().get.output

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 5)
    assert[String](df, "fa_recipient_type_description", SchoolTypeDesc)
    assert[Int](df, "fa_recipient_type", SchoolTypeId)
    assertCommonFields(PrincipalRoleName, ActiveStatus, df)
    assert[String](df, "fa_recipient_id", "74a9f731-4545-4fcf-8f9b-2aea397f69f1")
  }

  test("transform principal announcement event without school successfully") {
    val value =
      """
        |[
        |{
        |  "eventType": "PrincipalAnnouncementSentEvent",
        |  "announcementId": "65cf3dd8-b819-446d-b0bf-b7216b7bbba9",
        |  "principalId": "c3bed03b-19ff-4718-b6bf-17a359a349cd",
        |  "schoolId": null,
        |  "gradeIds": ["grade_uuid1"],
        |  "classIds": null,
        |  "studentIds": null,
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

    val transformer = new PrincipalAnnouncementTransform(sprk, service)
    val inputDF = spark.read.json(Seq(value).toDS())
      .withColumn(ClassIdsColName, lit(null).cast(ArrayType(StringType)))
      .withColumn(StudentIdsColName, lit(null).cast(ArrayType(StringType)))
    when(service.readOptional(ParquetPrincipalAnnouncementSource, sprk)).thenReturn(Some(inputDF))

    val df = transformer.transform().get.output

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 1)
    assert[String](df, "fa_recipient_type_description", GradeTypeDesc)
    assert[Int](df, "fa_recipient_type", GradeTypeId)
    assertCommonFields(PrincipalRoleName, ActiveStatus, df)
    assert[String](df, "fa_recipient_id", "grade_uuid1")
  }
}
