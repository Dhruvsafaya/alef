package com.alefeducation.warehouse

import com.alefeducation.warehouse.models.WarehouseConnection
import com.alefeducation.warehouse.pathway.LevelsRecommendedTransformer
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scalikejdbc.AutoSession

class LevelsRecommendedTransformerTest extends AnyFunSuite with Matchers {
  test("should prepare queries") {

    val levelRecommendedTransformer = LevelsRecommendedTransformer
    val connection = WarehouseConnection("testalefdw", "http://localhost:8080", "jdbc", "username", "password")
    implicit val autoSession: AutoSession = AutoSession
    val queryMetas = levelRecommendedTransformer.prepareQueries(connection)

    val expectedSelectStatement =
      s"""
         |select
         |     flr_dw_id
         |from testalefdw_stage.staging_levels_recommended slr
         |inner join testalefdw.dim_tenant t on t.tenant_id = slr.flr_tenant_id
         |inner join testalefdw_stage.rel_user s on s.user_id = slr.flr_student_id
         |inner join testalefdw_stage.rel_dw_id_mappings c on c.id = slr.flr_class_id and c.entity_type = 'class'
         |inner join testalefdw_stage.rel_dw_id_mappings pc on pc.id = slr.flr_pathway_id and pc.entity_type = 'course'
         |inner join testalefdw_stage.rel_dw_id_mappings plc on plc.id = slr.flr_level_id and plc.entity_type = 'course_activity_container'
         |left join testalefdw_stage.rel_dw_id_mappings plc1 on plc1.id = slr.flr_completed_level_id and plc1.entity_type = 'course_activity_container'
         |order by flr_dw_id
         |limit 60000
         |""".stripMargin

    queryMetas.head.stagingTable should be("staging_levels_recommended")
    queryMetas.head.selectSQL.stripMargin should be(expectedSelectStatement)
  }
}
