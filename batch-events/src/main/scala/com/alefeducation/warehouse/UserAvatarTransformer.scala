package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.CommonTransformer

object UserAvatarTransformer extends CommonTransformer {

  override def getPkColumn(): String = "fua_dw_id"

  override def getStagingTableName(): String = "staging_user_avatar"

  val commonColumns = List(
    "fua_created_time",
    "fua_dw_created_time",
    "fua_date_dw_id",
    "fua_dw_id",
    "fua_id",
    "fua_tenant_id",
    "fua_user_id",
    "fua_school_id",
    "fua_grade_id",
    "fua_avatar_file_id",
    "fua_avatar_type"
  )

  val dwIds = List(
    "fua_tenant_dw_id",
    "fua_user_dw_id",
    "fua_school_dw_id",
    "fua_grade_dw_id",
    "fua_avatar_dw_id"
  )

  val uuids = List(
    "t.tenant_dw_id AS fua_tenant_dw_id",
    "u.user_dw_id AS fua_user_dw_id",
    "s.school_dw_id AS fua_school_dw_id",
    "g.grade_dw_id AS fua_grade_dw_id",
    "a.avatar_dw_id AS fua_avatar_dw_id"
  )

  val insertColumns = commonColumns ++ dwIds
  val selectColumns = commonColumns ++ uuids

  override def getSelectQuery(schema: String): String =
    s"""
       |SELECT
       |     ${getPkColumn()}
       |${fromStatement(schema)}
       |ORDER BY ${getPkColumn()}
       |LIMIT $QUERY_LIMIT
       |""".stripMargin

  override def getInsertFromSelectQuery(schema: String, ids: List[Long]): String = {
    val idCols = ids.mkString(",")
    val insertColumnsStr = insertColumns.mkString("\n\t", ",\n\t", "\n")
    val selectColumnsStr = selectColumns.mkString("\n\t", ",\n\t", "\n")
    s"""
       |INSERT INTO $schema.fact_user_avatar ($insertColumnsStr)
       |SELECT $selectColumnsStr
       |${fromStatement(schema)}
       |WHERE ${getPkColumn()} IN ($idCols)
       |""".stripMargin
  }

  private def fromStatement(schema: String): String =
    s"""
       |FROM ${schema}_stage.staging_user_avatar ua
       |LEFT JOIN $schema.dim_grade g ON g.grade_id = ua.fua_grade_id
       |INNER JOIN $schema.dim_tenant t ON t.tenant_id = ua.fua_tenant_id
       |INNER JOIN $schema.dim_school s ON s.school_id = ua.fua_school_id
       |INNER JOIN ${schema}_stage.rel_user u ON u.user_id = ua.fua_user_id
       |LEFT JOIN ${schema}.dim_avatar a ON a.avatar_id = ua.fua_id
       |""".stripMargin
}
