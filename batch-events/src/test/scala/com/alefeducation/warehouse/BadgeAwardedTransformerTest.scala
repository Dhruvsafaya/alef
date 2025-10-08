package com.alefeducation.warehouse

import com.alefeducation.warehouse.models.WarehouseConnection
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scalikejdbc.AutoSession

class BadgeAwardedTransformerTest  extends AnyFunSuite with Matchers {

  val transformer = BadgeAwardedTransformer
  val connection = WarehouseConnection("testalefdw", "http://localhost:8080", "jdbc", "username", "password")

  test("should prepare query") {
    implicit val autoSession: AutoSession = AutoSession
    val queryMetas = transformer.prepareQueries(connection)
    val expectedSelectStatement =
      s"""SELECT
         |   	fba_dw_id
         |FROM testalefdw_stage.staging_badge_awarded ba
         | INNER JOIN testalefdw.dim_grade g ON g.grade_id = ba.fba_grade_id
         | INNER JOIN testalefdw.dim_badge b ON ba.fba_badge_type_id = b.bdg_id AND ba.fba_tier = b.bdg_tier
         |     AND g.grade_k12grade = b.bdg_grade AND b.bdg_status = 1
         | INNER JOIN testalefdw.dim_tenant t ON ba.fba_tenant_id = t.tenant_id
         | INNER JOIN testalefdw.dim_school s ON s.school_id = ba.fba_school_id
         | INNER JOIN testalefdw_stage.rel_user u ON ba.fba_student_id = u.user_id
         | LEFT JOIN testalefdw.dim_academic_year day ON day.academic_year_id = ba.fba_academic_year_id
         |     AND day.academic_year_status = 1 AND day.academic_year_is_roll_over_completed = false
         | LEFT OUTER JOIN testalefdw.dim_section sec ON sec.section_id = ba.fba_section_id
         |WHERE
         | (ba.fba_academic_year_id IS NULL AND day.academic_year_dw_id IS NULL) OR
         | (ba.fba_academic_year_id IS NOT NULL AND day.academic_year_dw_id IS NOT NULL)
         |ORDER BY fba_dw_id
         |LIMIT 60000""".stripMargin

    queryMetas.head.stagingTable should be("staging_badge_awarded")
    queryMetas.head.selectSQL.stripMargin.trim should be(expectedSelectStatement)
  }

  test("should prepare select statement") {
    val cols = List(
      "fba_badge_dw_id",
      "fba_tenant_dw_id",
      "fba_grade_dw_id",
      "fba_school_dw_id",
      "fba_student_dw_id",
      "fba_academic_year_dw_id",
      "fba_section_dw_id",
      "fba_content_repository_dw_id",
      "fba_organization_dw_id",
      "fba_any_col"
    )
    val expRes =
      """b.bdg_dw_id AS fba_badge_dw_id,
        |	t.tenant_dw_id AS fba_tenant_dw_id,
        |	g.grade_dw_id AS fba_grade_dw_id,
        |	s.school_dw_id AS fba_school_dw_id,
        |	u.user_dw_id AS fba_student_dw_id,
        |	day.academic_year_dw_id AS fba_academic_year_dw_id,
        |	sec.section_dw_id AS fba_section_dw_id,
        |	s.school_content_repository_dw_id AS fba_content_repository_dw_id,
        |	s.school_organization_dw_id AS fba_organization_dw_id,
        |	fba_any_col""".stripMargin

    val actual = transformer.makeColumnNames(cols)

    actual.trim should be(expRes)
  }
}
