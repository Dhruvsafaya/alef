package com.alefeducation.warehouse

import com.alefeducation.util.StringUtilities.replaceSpecChars
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class PacingGuideTransformerTest extends AnyFunSuite with Matchers  {
  val transformer = PacingGuideTransformer

  test("should prepare select query") {
    val query = transformer.getSelectQuery("alefdw")
    val expectedQuery =
      s"""
         |SELECT pacing_dw_id FROM alefdw_stage.rel_pacing_guide staging JOIN alefdw.dim_tenant ON dim_tenant.tenant_id = staging.pacing_tenant_id JOIN alefdw_stage.rel_dw_id_mappings c ON c.id = staging.pacing_class_id and c.entity_type = 'class' JOIN alefdw_stage.rel_dw_id_mappings p ON p.id = staging.pacing_course_id and p.entity_type = 'course' JOIN (select lo_dw_id as act_dw_id, lo_id as act_id from alefdw.dim_learning_objective union select ic_dw_id as act_dw_id, ic_id as act_id from alefdw.dim_interim_checkpoint ) AS act ON act.act_id = staging.pacing_activity_id ORDER BY pacing_dw_id LIMIT 60000
         |""".stripMargin

    replaceSpecChars(query) shouldBe replaceSpecChars(expectedQuery)
  }
}
