package com.alefeducation.warehouse

import com.alefeducation.warehouse.models.WarehouseConnection
import com.alefeducation.warehouse.pathway.LevelCompletedTransformer
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scalikejdbc.AutoSession

class LevelCompletedTransformerTest extends AnyFunSuite with Matchers {
  test("should prepare queries") {

    val levelCompletedTransformer = LevelCompletedTransformer
    val connection = WarehouseConnection("testalefdw", "http://localhost:8080", "jdbc", "username", "password")
    implicit val autoSession: AutoSession = AutoSession
    val queryMetas = levelCompletedTransformer.prepareQueries(connection)

    val expectedSelectStatement =
      s"""
         |select
         |     flc_dw_id
         |from testalefdw_stage.staging_level_completed lc
         |inner join testalefdw.dim_tenant t on t.tenant_id = lc.flc_tenant_id
         |inner join testalefdw_stage.rel_user s on s.user_id = lc.flc_student_id
         |inner join testalefdw_stage.rel_dw_id_mappings c on c.id = lc.flc_class_id and c.entity_type = 'class'
         |inner join testalefdw_stage.rel_dw_id_mappings pc on pc.id = lc.flc_pathway_id and pc.entity_type = 'course'
         |inner join testalefdw_stage.rel_dw_id_mappings plc on plc.id = lc.flc_level_id and plc.entity_type = 'course_activity_container'
         |
         |order by flc_dw_id
         |limit 60000
         |""".stripMargin

    queryMetas.head.stagingTable should be("staging_level_completed")
    queryMetas.head.selectSQL.stripMargin should be(expectedSelectStatement)
  }
}
