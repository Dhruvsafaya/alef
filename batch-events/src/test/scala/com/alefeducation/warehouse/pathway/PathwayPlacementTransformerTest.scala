package com.alefeducation.warehouse.pathway

import com.alefeducation.warehouse.models.WarehouseConnection
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scalikejdbc.AutoSession

class PathwayPlacementTransformerTest extends AnyFunSuite with Matchers {
  val transformer = PathwayPlacementTransformer
  val connection = WarehouseConnection("testalefdw", "http://localhost:8080", "jdbc", "username", "password")

  test("should prepare query for pathway placement") {
    implicit val autoSession: AutoSession = AutoSession
    val queryMetas = transformer.prepareQueries(connection)
    val expectedSelectStatement =
      s"""select
         |     fpp_staging_id
         |from testalefdw_stage.staging_pathway_placement
         |left join testalefdw_stage.rel_user user1 on user1.user_id = staging_pathway_placement.fpp_created_by and user1.user_type = 'TEACHER'
         |inner join testalefdw_stage.rel_dw_id_mappings pc on pc.id = staging_pathway_placement.fpp_pathway_id and pc.entity_type = 'course'
         |inner join testalefdw.dim_tenant t on t.tenant_id = staging_pathway_placement.fpp_tenant_id
         |inner join testalefdw_stage.rel_dw_id_mappings c on c.id = staging_pathway_placement.fpp_class_id and c.entity_type = 'class'
         |inner join testalefdw_stage.rel_user user2 on user2.user_id = staging_pathway_placement.fpp_student_id where user2.user_type = 'STUDENT'
         |
         |order by fpp_staging_id
         |limit 60000""".stripMargin

    queryMetas.head.stagingTable should be("staging_pathway_placement")
    queryMetas.head.selectSQL.stripMargin.trim should be(expectedSelectStatement)
  }

  test("should prepare select statement for pathway placement") {
    val cols = List(
      "fpp_dw_created_time",
      "fpp_created_by_dw_id",
      "fpp_course_dw_id",
      "fpp_tenant_dw_id",
      "fpp_class_dw_id",
      "fpp_student_dw_id"
    )
    val expRes =
      """getdate() as fpp_dw_created_time,
        |user1.user_dw_id as fpp_created_by_dw_id,
        |pc.dw_id as fpp_course_dw_id,
        |t.tenant_dw_id as fpp_tenant_dw_id,
        |c.dw_id as fpp_class_dw_id,
        |user2.user_dw_id as fpp_student_dw_id""".stripMargin

    val actual = transformer.makeColumnNames(cols)

    actual.trim should be(expRes)
  }
}
