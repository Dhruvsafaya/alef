package com.alefeducation.warehouse

import com.alefeducation.util.StringUtilities.replaceSpecChars
import com.alefeducation.warehouse.models.WarehouseConnection
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class UserAvatarTransformerTest extends AnyFunSuite with Matchers {

  val transformer = UserAvatarTransformer
  val connection = WarehouseConnection("testalefdw", "http://localhost:8080", "jdbc", "username", "password")
  val schema = "testalefdw"

  test("should prepare query") {
    val selectQuery = transformer.getSelectQuery(connection.schema)
    val expectedSelectQuery =
      s"""
         |SELECT
         |  fua_dw_id
         |FROM testalefdw_stage.staging_user_avatar ua
         |  LEFT JOIN testalefdw.dim_grade g ON g.grade_id = ua.fua_grade_id
         |  INNER JOIN testalefdw.dim_tenant t ON t.tenant_id = ua.fua_tenant_id
         |  INNER JOIN testalefdw.dim_school s ON s.school_id = ua.fua_school_id
         |  INNER JOIN testalefdw_stage.rel_user u ON u.user_id = ua.fua_user_id
         |  LEFT JOIN testalefdw.dim_avatar a ON a.avatar_id = ua.fua_id
         |ORDER BY fua_dw_id
         |LIMIT 60000
         |""".stripMargin

    transformer.getStagingTableName() should be("staging_user_avatar")
    transformer.getPkColumn() should be("fua_dw_id")

    replaceSpecChars(selectQuery) should be(replaceSpecChars(expectedSelectQuery))


    val expectedInsertQuery =
      """
        |
        |
        |INSERT INTO testalefdw.fact_user_avatar (
        |	fua_created_time,
        |	fua_dw_created_time,
        |	fua_date_dw_id,
        |	fua_dw_id,
        |	fua_id,
        |	fua_tenant_id,
        |	fua_user_id,
        |	fua_school_id,
        |	fua_grade_id,
        | fua_avatar_file_id,
        |	fua_avatar_type,
        |	fua_tenant_dw_id,
        |	fua_user_dw_id,
        |	fua_school_dw_id,
        |	fua_grade_dw_id,
        | fua_avatar_dw_id
        |)
        |
        |SELECT
        |	fua_created_time,
        |	fua_dw_created_time,
        |	fua_date_dw_id,
        |	fua_dw_id,
        |	fua_id,
        |	fua_tenant_id,
        |	fua_user_id,
        |	fua_school_id,
        |	fua_grade_id,
        | fua_avatar_file_id,
        |	fua_avatar_type,
        |	t.tenant_dw_id AS fua_tenant_dw_id,
        |	u.user_dw_id AS fua_user_dw_id,
        |	s.school_dw_id AS fua_school_dw_id,
        |	g.grade_dw_id AS fua_grade_dw_id,
        |	a.avatar_dw_id AS fua_avatar_dw_id
        |FROM testalefdw_stage.staging_user_avatar ua
        |	LEFT JOIN testalefdw.dim_grade g ON g.grade_id = ua.fua_grade_id
        |	INNER JOIN testalefdw.dim_tenant t ON t.tenant_id = ua.fua_tenant_id
        |	INNER JOIN testalefdw.dim_school s ON s.school_id = ua.fua_school_id
        |	INNER JOIN testalefdw_stage.rel_user u ON u.user_id = ua.fua_user_id
        |	LEFT JOIN testalefdw.dim_avatar a ON a.avatar_id = ua.fua_id
        |WHERE fua_dw_id IN (3003,3004,3005)
        |""".stripMargin

    val insertQuery = transformer.getInsertFromSelectQuery(schema, List(3003, 3004, 3005))

    replaceSpecChars(insertQuery) should be(replaceSpecChars(expectedInsertQuery))
  }
}
