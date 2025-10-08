package com.alefeducation.facts

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.facts.CertificateAwardedTransform.sourceName
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class CertificateAwardedTransformSpec extends SparkSuite {

  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  val expectedColumns: Set[String] = Set(
    "fsca_created_time",
    "fsca_dw_created_time",
    "fsca_date_dw_id",
    "eventdate",
    "fsca_certificate_id",
    "fsca_student_id",
    "fsca_academic_year_id",
    "fsca_grade_id",
    "fsca_class_id",
    "fsca_teacher_id",
    "fsca_award_category",
    "fsca_award_purpose",
    "fsca_language",
    "fsca_tenant_id"
  )

  test("transform certificate awarded event successfully") {
    val value = """
                  |[
                  |{
                  |  "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013",
                  |  "eventType": "StudentBadgeAwardedEvent",
                  |  "loadtime": "2023-04-27 10:53:13.675",
                  |  "occurredOn": "2023-04-27 10:01:38.373481108",
                  |  "eventDateDw": "20230427",
                  |	 "certificateId": "certificateId",
                  |	 "studentId": "studentId",
                  |	 "academicYearId": "academicYearId",
                  |	 "gradeId": "gradeId",
                  |	 "classId": "classId",
                  |	 "awardedBy": "awardedBy",
                  |	 "category": "category",
                  |	 "purpose": "purpose",
                  |	 "language": "language"
                  |}
                  |]
        """.stripMargin
    val sprk = spark
    import sprk.implicits._

    val transformer = new CertificateAwardedTransform(sprk, service)

    val inputDF = spark.read.json(Seq(value).toDS())
    when(service.readOptional(sourceName, sprk)).thenReturn(Some(inputDF))

    val df = transformer.transform().get.output

    assert(df.columns.toSet === expectedColumns)
    assert[String](df, "fsca_created_time", "2023-04-27 10:01:38.373481")
    assert[Long](df, "fsca_date_dw_id", 20230427)
    assert[String](df, "eventdate", "2023-04-27")
    assert[String](df, "fsca_tenant_id", "93e4949d-7eff-4707-9201-dac917a5e013")
    assert[String](df, "fsca_certificate_id", "certificateId")
    assert[String](df, "fsca_student_id", "studentId")
    assert[String](df, "fsca_academic_year_id", "academicYearId")
    assert[String](df, "fsca_grade_id", "gradeId")
    assert[String](df, "fsca_class_id", "classId")
    assert[String](df, "fsca_teacher_id", "awardedBy")
    assert[String](df, "fsca_award_category", "category")
    assert[String](df, "fsca_award_purpose", "purpose")
    assert[String](df, "fsca_language", "language")
  }
}
