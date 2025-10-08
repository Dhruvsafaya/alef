package com.alefeducation.warehouse

import com.alefeducation.warehouse.models.WarehouseConnection
import com.alefeducation.warehouse.pathway.LevelCompletedTransformer
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scalikejdbc.AutoSession

class FactUserLoginTransformerTest extends AnyFunSuite with Matchers {
  test("should prepare queries") {

    val loginTransformer = FactUserLoginTransformer
    val connection = WarehouseConnection("testalefdw", "http://localhost:8080", "jdbc", "username", "password")
    implicit val autoSession: AutoSession = AutoSession
    val queryMetas = loginTransformer.prepareQueries(connection)

    val expectedSelectStatement =
      s"""
         |select
         |     ful_staging_id
         |from testalefdw_stage.staging_user_login
         |inner join testalefdw_stage.rel_user user1 on user1.user_id = staging_user_login.user_uuid
         |inner join testalefdw.dim_role on dim_role.role_name = staging_user_login.role_uuid
         |left join testalefdw.dim_tenant on dim_tenant.tenant_id = staging_user_login.tenant_uuid
         |left join testalefdw.dim_school on dim_school.school_id = staging_user_login.school_uuid
         |
         |order by ful_staging_id
         |limit 60000
         |""".stripMargin

    queryMetas.head.stagingTable should be("staging_user_login")
    queryMetas.head.selectSQL.stripMargin should be(expectedSelectStatement)
  }
}
