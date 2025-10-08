package com.alefeducation.warehouse

import com.alefeducation.warehouse.models.WarehouseConnection
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scalikejdbc.AutoSession
import com.alefeducation.warehouse.models.WarehouseConnection

class WeeklyGoalTransformerTest extends AnyFunSuite with Matchers {
  test("should prepare queries") {

    val weeklyGoalTransformer = WeeklyGoalTransformer
    val connection = WarehouseConnection("testalefdw", "http://localhost:8080", "jdbc", "username", "password")
    implicit val autoSession: AutoSession = AutoSession
    val queryMetas = weeklyGoalTransformer.prepareQueries(connection)

    val expectedSelectStatement =
      s"""
         |select
         |     fwg_dw_id
         |from testalefdw_stage.staging_weekly_goal g
         |inner join testalefdw.dim_weekly_goal_type wgt on wgt.weekly_goal_type_id = g.fwg_type_id
         |inner join testalefdw.dim_tenant t on t.tenant_id = g.fwg_tenant_id
         |inner join testalefdw_stage.rel_dw_id_mappings c on c.id = g.fwg_class_id and c.entity_type = 'class'
         |inner join testalefdw_stage.rel_user s on s.user_id = g.fwg_student_id
         |
         |order by fwg_dw_id
         |limit 60000
         |""".stripMargin


    queryMetas.head.stagingTable should be("staging_weekly_goal")
    queryMetas.head.selectSQL.stripMargin should be(expectedSelectStatement)
  }
}
