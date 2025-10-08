package com.alefeducation.warehouse

import com.alefeducation.util.StringUtilities.replaceSpecChars
import com.alefeducation.warehouse.models.WarehouseConnection
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scalikejdbc.AutoSession


class SkillContentUnavailableTransformerTest extends AnyFunSuite with Matchers {

  val transformer: SkillContentUnavailableTransformer.type = SkillContentUnavailableTransformer
  val connection: WarehouseConnection = WarehouseConnection("alefdw", "http://localhost:8080", "jdbc", "username", "password")

  test("should prepare query") {
    implicit val autoSession: AutoSession = AutoSession
    val queryMetas = transformer.prepareQueries(connection)

    val expectedInsertStatement =
      s"""
         |INSERT INTO alefdw.fact_skill_content_unavailable (scu_created_time, scu_dw_created_time, scu_date_dw_id, scu_tenant_dw_id, scu_lo_dw_id, scu_skill_dw_id) ( select scu_created_time, scu_dw_created_time, scu_date_dw_id, dim_tenant.tenant_dw_id, dim_learning_objective.lo_dw_id, dim_ccl_skill.ccl_skill_dw_id from alefdw_stage.staging_skill_content_unavailable inner join alefdw.dim_tenant on dim_tenant.tenant_id = staging_skill_content_unavailable.tenant_uuid inner join alefdw.dim_learning_objective on dim_learning_objective.lo_id = staging_skill_content_unavailable.lo_uuid inner join alefdw.dim_ccl_skill on dim_ccl_skill.ccl_skill_id = staging_skill_content_unavailable.skill_uuid order by scu_staging_id limit 60000 )
         |""".stripMargin

    queryMetas.head.stagingTable should be("staging_skill_content_unavailable")
    replaceSpecChars(queryMetas.head.insertSQL.stripMargin) should be(replaceSpecChars(expectedInsertStatement))
  }
}

