package com.alefeducation.warehouse

import com.alefeducation.warehouse.models.WarehouseConnection
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scalikejdbc.AutoSession

class HeartbeatAggregationTransformerTest  extends AnyFunSuite with Matchers {

  val transformer: HeartbeatAggregationTransformer.type = HeartbeatAggregationTransformer
  val connection: WarehouseConnection = WarehouseConnection(
    "testalefdw", "http://localhost:8080", "jdbc", "username", "password"
  )


  test("should prepare query") {
    implicit val autoSession: AutoSession = AutoSession

    val stagingTableName: String = "staging_user_heartbeat_hourly_aggregated"

    val queryMetas = transformer.prepareQueries(connection)
    val expectedSelectStatement =
      s"""
         |SELECT
         |  fuhha_staging_id
         |FROM (
         |  SELECT
         |     fuhha_staging_id
         |
         |  FROM testalefdw_stage.staging_user_heartbeat_hourly_aggregated staging
         |  INNER JOIN testalefdw.dim_role r ON UPPER(staging.fuhha_role) = UPPER(r.role_name)
         |  INNER JOIN testalefdw.dim_tenant t ON staging.fuhha_tenant_id = t.tenant_id
         |  INNER JOIN testalefdw_stage.rel_user u ON staging.fuhha_user_id = u.user_id
         |  INNER JOIN testalefdw.dim_school s ON staging.fuhha_school_id = s.school_id
         |
         |  ORDER BY fuhha_staging_id
         |  LIMIT 60000
         |)""".stripMargin

    queryMetas.head.stagingTable should be(stagingTableName)
    queryMetas.head.selectSQL.stripMargin.trim should be(expectedSelectStatement.trim)
  }

  test("should prepare select statement") {
    val cols = List(
      "fuhha_role_dw_id",
      "fuhha_tenant_dw_id",
      "fuhha_user_dw_id",
      "some_field"
    )
    val expRes =
      """r.role_dw_id AS fuhha_role_dw_id,
        |t.tenant_dw_id AS fuhha_tenant_dw_id,
        |u.user_dw_id AS fuhha_user_dw_id,
        |some_field""".stripMargin

    val actual = transformer.makeColumnNames(cols)

    actual.trim should be(expRes)


    val expResTeacher =
      """r.role_dw_id AS fuhha_role_dw_id,
        |t.tenant_dw_id AS fuhha_tenant_dw_id,
        |u.user_dw_id AS fuhha_user_dw_id,
        |some_field""".stripMargin

    val actualTeacher = transformer.makeColumnNames(cols)

    actualTeacher.trim should be(expResTeacher)
  }
}
