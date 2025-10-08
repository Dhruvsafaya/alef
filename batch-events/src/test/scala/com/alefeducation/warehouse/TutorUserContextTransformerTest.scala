package com.alefeducation.warehouse

import com.alefeducation.warehouse.models.WarehouseConnection
import com.alefeducation.warehouse.tutor.TutorUserContextTransformer
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scalikejdbc.AutoSession

class TutorUserContextTransformerTest extends AnyFunSuite with Matchers {
  val transformer = TutorUserContextTransformer
  val connection = WarehouseConnection("testalefdw", "http://localhost:8080", "jdbc", "username", "password")

  test("should prepare query") {
    implicit val autoSession: AutoSession = AutoSession
    val queryMetas = transformer.prepareQueries(connection)
    val expectedSelectStatement =
      s"""SELECT
         |   	ftc_dw_id
         |FROM testalefdw_stage.staging_tutor_user_context tuc
         |INNER JOIN testalefdw.dim_grade g ON g.grade_id = tuc.ftc_grade_id
         |INNER JOIN testalefdw.dim_tenant t ON t.tenant_id = tuc.ftc_tenant_id
         |INNER JOIN testalefdw.dim_school s ON s.school_id = tuc.ftc_school_id
         |LEFT JOIN testalefdw.dim_curriculum_subject csu ON csu.curr_subject_id = tuc.ftc_subject_id and csu.curr_subject_status=1
         |INNER JOIN testalefdw_stage.rel_user u ON u.user_id = tuc.ftc_user_id
         |ORDER BY ftc_dw_id
         |LIMIT 60000""".stripMargin

    queryMetas.head.stagingTable should be("staging_tutor_user_context")
    queryMetas.head.selectSQL.stripMargin.trim should be(expectedSelectStatement)
  }

  test("should prepare select statement") {
    val cols = List(
      "ftc_role",
      "ftc_context_id",
      "ftc_school_dw_id",
      "ftc_grade_dw_id",
      "ftc_grade",
      "ftc_subject_dw_id",
      "ftc_subject",
      "ftc_language",
      "ftc_tenant_dw_id",
      "ftc_created_time",
      "ftc_dw_created_time",
      "ftc_date_dw_id",
      "ftc_user_dw_id"
    )
    val expRes =
      """ftc_role,
        |	ftc_context_id,
        |	s.school_dw_id AS ftc_school_dw_id,
        |	g.grade_dw_id AS ftc_grade_dw_id,
        |	ftc_grade,
        |	csu.curr_subject_dw_id AS ftc_subject_dw_id,
        |	ftc_subject,
        |	ftc_language,
        |	t.tenant_dw_id AS ftc_tenant_dw_id,
        |	ftc_created_time,
        |	ftc_dw_created_time,
        |	ftc_date_dw_id,
        |	u.user_dw_id AS ftc_user_dw_id""".stripMargin

    val actual = transformer.makeColumnNames(cols)

    actual.trim should be(expRes)
  }
}
