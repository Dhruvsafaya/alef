package com.alefeducation.warehouse

import com.alefeducation.warehouse.models.WarehouseConnection
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scalikejdbc.AutoSession

class CertificateAwardedTransformerTest  extends AnyFunSuite with Matchers {

  val transformer = CertificateAwardedTransformer
  val connection = WarehouseConnection("testalefdw", "http://localhost:8080", "jdbc", "username", "password")

  test("should prepare query") {
    implicit val autoSession: AutoSession = AutoSession
    val queryMetas = transformer.prepareQueries(connection)
    val expectedSelectStatement =
      s"""SELECT
         |   	fsca_dw_id
         |FROM testalefdw_stage.staging_student_certificate_awarded staging
         | INNER JOIN testalefdw.dim_tenant t ON staging.fsca_tenant_id = t.tenant_id
         | INNER JOIN testalefdw_stage.rel_user s ON staging.fsca_student_id = s.user_id
         | LEFT JOIN testalefdw.dim_academic_year day ON staging.fsca_academic_year_id = day.academic_year_id
         |   AND day.academic_year_status = 1 AND day.academic_year_is_roll_over_completed = false
         | INNER JOIN testalefdw.dim_grade g ON staging.fsca_grade_id = g.grade_id
         | INNER JOIN testalefdw_stage.rel_dw_id_mappings c ON staging.fsca_class_id = c.id
         |   AND entity_type = 'class'
         | INNER JOIN testalefdw_stage.rel_user teacher ON staging.fsca_teacher_id = teacher.user_id
         |WHERE
         | (staging.fsca_academic_year_id IS NULL AND day.academic_year_dw_id IS NULL) OR
         | (staging.fsca_academic_year_id IS NOT NULL AND day.academic_year_dw_id IS NOT NULL)
         |ORDER BY fsca_dw_id
         |LIMIT 60000""".stripMargin

    queryMetas.head.stagingTable should be("staging_student_certificate_awarded")
    queryMetas.head.selectSQL.stripMargin.trim should be(expectedSelectStatement)
  }

  test("should prepare select statement") {
    val cols = List(
      "fsca_tenant_dw_id",
      "fsca_student_dw_id",
      "fsca_academic_year_dw_id",
      "fsca_grade_dw_id",
      "fsca_class_dw_id",
      "fsca_teacher_dw_id",
      "fsca_language"
    )
    val expRes =
      """t.tenant_dw_id AS fsca_tenant_dw_id,
        |	s.user_dw_id AS fsca_student_dw_id,
        |	day.academic_year_dw_id AS fsca_academic_year_dw_id,
        |	g.grade_dw_id AS fsca_grade_dw_id,
        |	c.dw_id AS fsca_class_dw_id,
        |	teacher.user_dw_id AS fsca_teacher_dw_id,
        |	fsca_language""".stripMargin

    val actual = transformer.makeColumnNames(cols)

    actual.trim should be(expRes)
  }
}
