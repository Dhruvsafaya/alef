package com.alefeducation.warehouse.pathway

import com.alefeducation.warehouse.models.WarehouseConnection
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scalikejdbc.AutoSession

class PathwayLeaderboardTransformerTest extends AnyFunSuite with Matchers {

  val transformer = PathwayLeaderboardTransformer
  val connection = WarehouseConnection("testalefdw", "http://localhost:8080", "jdbc", "username", "password")

  test("should prepare query") {
    implicit val autoSession: AutoSession = AutoSession
    val queryMetas = transformer.prepareQueries(connection)
    val expectedSelectStatement =
      s"""SELECT
         |     	fpl_staging_id
         |FROM testalefdw_stage.staging_pathway_leaderboard pl
         | INNER JOIN testalefdw.dim_tenant t ON t.tenant_id = pl.fpl_tenant_id
         | INNER JOIN testalefdw_stage.rel_user u ON u.user_id = pl.fpl_student_id
         | INNER JOIN testalefdw_stage.rel_dw_id_mappings c ON c.id = pl.fpl_class_id and c.entity_type = 'class'
         | INNER JOIN testalefdw_stage.rel_dw_id_mappings pc ON pc.id = pl.fpl_pathway_id and pc.entity_type = 'course'
         | INNER JOIN testalefdw.dim_grade g ON g.grade_id = pl.fpl_grade_id
         | LEFT JOIN testalefdw.dim_academic_year day ON day.academic_year_id = pl.fpl_academic_year_id AND day.academic_year_status = 1
         |WHERE
         |  (pl.fpl_academic_year_id IS NULL AND day.academic_year_dw_id IS NULL) OR
         |  (pl.fpl_academic_year_id IS NOT NULL AND day.academic_year_dw_id IS NOT NULL)
         |ORDER BY fpl_staging_id
         |LIMIT 60000""".stripMargin

    queryMetas.head.stagingTable should be("staging_pathway_leaderboard")
    queryMetas.head.selectSQL.stripMargin.trim should be(expectedSelectStatement)
  }

  test("should prepare select statement") {
    val cols = List(
      "fpl_tenant_dw_id",
      "fpl_grade_dw_id",
      "fpl_student_dw_id",
      "fpl_academic_year_dw_id",
      "fpl_class_dw_id",
      "fpl_course_dw_id",
      "fpl_any_col"
    )
    val expRes =
      """t.tenant_dw_id AS fpl_tenant_dw_id,
        |	g.grade_dw_id AS fpl_grade_dw_id,
        |	u.user_dw_id AS fpl_student_dw_id,
        |	day.academic_year_dw_id AS fba_academic_year_dw_id,
        |	c.dw_id AS fpl_class_dw_id,
        |	pc.dw_id AS fpl_course_dw_id,
        |	fpl_any_col""".stripMargin

    val actual = transformer.makeColumnNames(cols)

    actual.trim should be(expRes)
  }
}
