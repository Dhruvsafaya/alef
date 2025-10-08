package com.alefeducation.warehouse

import com.alefeducation.warehouse.models.WarehouseConnection
import com.alefeducation.warehouse.tutor.TutorSessionTransformer
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scalikejdbc.AutoSession

class TutorSessionTransformerTest extends AnyFunSuite with Matchers {
  val transformer = TutorSessionTransformer
  val connection = WarehouseConnection("testalefdw", "http://localhost:8080", "jdbc", "username", "password")

  test("should prepare query") {
    implicit val autoSession: AutoSession = AutoSession
    val queryMetas = transformer.prepareQueries(connection)
    val expectedSelectStatement =
      s"""SELECT
         |   	fts_dw_id
         |FROM testalefdw_stage.staging_tutor_session ts
         |INNER JOIN testalefdw.dim_grade g ON g.grade_id = ts.fts_grade_id
         |INNER JOIN testalefdw.dim_tenant t ON t.tenant_id = ts.fts_tenant_id
         |INNER JOIN testalefdw.dim_school s ON s.school_id = ts.fts_school_id
         |INNER JOIN testalefdw.dim_curriculum_subject csu ON csu.curr_subject_id = ts.fts_subject_id and csu.curr_subject_status=1
         |INNER JOIN testalefdw_stage.rel_user u ON u.user_id = ts.fts_user_id
         |INNER JOIN testalefdw.dim_learning_objective lo ON lo.lo_id = ts.fts_activity_id
         |LEFT JOIN testalefdw_stage.rel_dw_id_mappings plc ON plc.id = ts.fts_level_id and plc.entity_type = 'course_activity_container'
         |LEFT JOIN testalefdw.dim_outcome oc on oc.outcome_id = ts.fts_outcome_id
         |ORDER BY fts_dw_id
         |LIMIT 60000""".stripMargin

    queryMetas.head.stagingTable should be("staging_tutor_session")
    queryMetas.head.selectSQL.stripMargin.trim should be(expectedSelectStatement)
  }

  test("should prepare select statement") {
    val cols = List(
      "fts_role",
      "fts_context_id",
      "fts_school_dw_id",
      "fts_grade_dw_id",
      "fts_grade",
      "fts_subject_dw_id",
      "fts_subject",
      "fts_language",
      "fts_tenant_dw_id",
      "fts_created_time",
      "fts_dw_created_time",
      "fts_date_dw_id",
      "fts_user_dw_id",
      "fts_session_id",
      "fts_session_state",
      "fts_activity_dw_id",
      "fts_activity_status",
      "fts_material_id",
      "fts_material_type",
      "fts_course_activity_container_dw_id",
      "fts_outcome_dw_id"
    )
    val expRes = """fts_role,
                   |	fts_context_id,
                   |	s.school_dw_id AS fts_school_dw_id,
                   |	g.grade_dw_id AS fts_grade_dw_id,
                   |	fts_grade,
                   |	csu.curr_subject_dw_id AS fts_subject_dw_id,
                   |	fts_subject,
                   |	fts_language,
                   |	t.tenant_dw_id AS fts_tenant_dw_id,
                   |	fts_created_time,
                   |	fts_dw_created_time,
                   |	fts_date_dw_id,
                   |	u.user_dw_id AS fts_user_dw_id,
                   |	fts_session_id,
                   |	fts_session_state,
                   |	lo.lo_dw_id AS fts_activity_dw_id,
                   |	fts_activity_status,
                   |	fts_material_id,
                   |	fts_material_type,
                   |	plc.dw_id AS fts_course_activity_container_dw_id,
                   |	oc.outcome_dw_id AS fts_outcome_dw_id""".stripMargin

    val actual = transformer.makeColumnNames(cols)
    actual.trim should be(expRes)
  }
}
