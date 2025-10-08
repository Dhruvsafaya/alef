package com.alefeducation.warehouse

import com.alefeducation.warehouse.models.WarehouseConnection
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scalikejdbc.AutoSession

class AnnouncementTransformerTest extends AnyFunSuite with Matchers {

  val transformer = AnnouncementTransformer
  val connection = WarehouseConnection("testalefdw", "http://localhost:8080", "jdbc", "username", "password")

  test("should prepare query") {
    implicit val autoSession: AutoSession = AutoSession
    val queryMetas = transformer.prepareQueries(connection)
    val expectedSelectStatement =
      s"""SELECT
         |  	fa_dw_id
         |FROM testalefdw_stage.staging_announcement a
         |INNER JOIN testalefdw.dim_role r ON r.role_name = a.fa_role_id
         |INNER JOIN testalefdw.dim_tenant t ON t.tenant_id = a.fa_tenant_id
         |INNER JOIN testalefdw_stage.rel_user u ON u.user_id = a.fa_admin_id
         |LEFT JOIN testalefdw.dim_school s ON s.school_id = a.fa_recipient_id and a.fa_recipient_type = 1
         |LEFT JOIN testalefdw.dim_grade g ON g.grade_id = a.fa_recipient_id and a.fa_recipient_type = 2
         |LEFT JOIN testalefdw_stage.rel_dw_id_mappings c
         |          ON c.id = a.fa_recipient_id and a.fa_recipient_type = 3 and c.entity_type = 'class'
         |LEFT JOIN testalefdw_stage.rel_user us ON us.user_id = a.fa_recipient_id and a.fa_recipient_type = 4
         |WHERE s.school_id IS NOT NULL OR g.grade_id IS NOT NULL OR c.id IS NOT NULL OR us.user_id IS NOT NULL
         |ORDER BY fa_dw_id
         |LIMIT 60000""".stripMargin

    queryMetas.head.stagingTable should be("staging_announcement")
    queryMetas.head.selectSQL.stripMargin.trim should be(expectedSelectStatement)
  }

  test("should prepare select statement") {
    val cols = List(
      "fa_role_dw_id",
      "fa_admin_dw_id",
      "fa_tenant_dw_id",
      "fa_recipient_dw_id",
      "fa_any_other_col"
    )
    val expRes =
      """r.role_dw_id AS fa_role_dw_id,
        |	u.user_dw_id AS fa_admin_dw_id,
        |	t.tenant_dw_id AS fa_tenant_dw_id,
        |DECODE(a.fa_recipient_type,
        |       1, s.school_dw_id,
        |       2, g.grade_dw_id,
        |       3, c.dw_id,
        |       4, us.user_dw_id
        |       ) AS fa_recipient_dw_id
        |,
        |	fa_any_other_col""".stripMargin

    val actual = transformer.makeColumnNames(cols)

    actual.trim should be(expRes)
  }
}