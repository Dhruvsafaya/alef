package com.alefeducation.warehouse

import com.alefeducation.warehouse.models.WarehouseConnection
import com.alefeducation.warehouse.pathway.PathwayActivityCompletedTransformer
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scalikejdbc.AutoSession

class PathwayActivityCompletedTransformerTest extends AnyFunSuite with Matchers {
  test("should prepare queries") {

    val pathwayActivityCompletedTransformer = PathwayActivityCompletedTransformer
    val connection = WarehouseConnection("testalefdw", "http://localhost:8080", "jdbc", "username", "password")
    implicit val autoSession: AutoSession = AutoSession
    val queryMetas = pathwayActivityCompletedTransformer.prepareQueries(connection)

    val expectedSelectStatement =
      s"""
         |select
         |     fpac_dw_id
         |from testalefdw_stage.staging_pathway_activity_completed pac
         |inner join testalefdw.dim_tenant t on t.tenant_id = pac.fpac_tenant_id
         |inner join testalefdw_stage.rel_user s on s.user_id = pac.fpac_student_id
         |inner join testalefdw_stage.rel_dw_id_mappings c on c.id = pac.fpac_class_id and c.entity_type = 'class'
         |inner join testalefdw_stage.rel_dw_id_mappings pc on pc.id = pac.fpac_pathway_id and pc.entity_type = 'course'
         |inner join testalefdw_stage.rel_dw_id_mappings plc on plc.id = pac.fpac_level_id and plc.entity_type = 'course_activity_container'
         |left join testalefdw.dim_learning_objective lesson on lesson.lo_id = pac.fpac_activity_id and pac.fpac_activity_type = 1
         |left join testalefdw.dim_interim_checkpoint ic on ic.ic_id = pac.fpac_activity_id and pac.fpac_activity_type = 2
         |
         |where
         |(pac.fpac_activity_type = 1 and pac.fpac_activity_id is not null and lesson.lo_id is not null) or
         |(pac.fpac_activity_type = 2 and pac.fpac_activity_id is not null and ic.ic_id is not null)
         |
         |order by fpac_dw_id
         |limit 60000
         |""".stripMargin

    queryMetas.head.stagingTable should be("staging_pathway_activity_completed")
    queryMetas.head.selectSQL.stripMargin should be(expectedSelectStatement)
  }
  test("should prepare select statement") {
    val cols = List(
      "fpac_tenant_dw_id",
      "fpac_student_dw_id",
      "fpac_class_dw_id",
      "fpac_course_dw_id",
      "fpac_activity_dw_id",
    )
    val expRes = "t.tenant_dw_id as fpac_tenant_dw_id," +
      " s.user_dw_id as fpac_student_dw_id," +
      " c.dw_id as fpac_class_dw_id," +
      " pc.dw_id as fpac_course_dw_id," +
      " CASE WHEN pac.fpac_activity_type = 1 THEN lesson.lo_dw_id ELSE ic.ic_dw_id END AS fpac_activity_dw_id"


    val actual = PathwayActivityCompletedTransformer.makeColumnNames(cols)

    actual.trim should be(expRes)
  }
}
