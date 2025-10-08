package com.alefeducation.warehouse.staff

import com.alefeducation.warehouse.core.CommonTransformer

object StaffUserTransformer extends CommonTransformer {

  private val mainColNames: List[String] = List(
    "rel_staff_user_dw_id",
    "staff_user_created_time",
    "staff_user_active_until",
    "staff_user_dw_created_time",
    "staff_user_status",
    "staff_user_id",
    "staff_user_dw_id",
    "staff_user_onboarded",
    "staff_user_expirable",
    "staff_user_exclude_from_report",
    "staff_user_avatar",
    "staff_user_event_type",
    "staff_user_enabled"
  )

  override def getSelectQuery(schema: String): String = {
    val staffUserPkCol = getPkColumn()
    val from = fromStatement(schema)

    s"SELECT $staffUserPkCol $from ORDER BY $staffUserPkCol LIMIT $QUERY_LIMIT"
  }

  override def getInsertFromSelectQuery(schema: String, ids: List[Long]): String = {
    val idsInStatement = ids.mkString(",")
    val insCols = mainColNames.mkString("\n\t", ",\n\t", "\n")
    val selCols = makeColNames(mainColNames)
    s"""
       |INSERT INTO $schema.dim_staff_user ($insCols)
       |SELECT $selCols
       |${fromStatement(schema)}
       |WHERE staging.$getPkColumn IN ($idsInStatement)
       |""".stripMargin
  }

  override def getPkColumn(): String = "rel_staff_user_dw_id"

  override def getStagingTableName(): String = "rel_staff_user"

  private def fromStatement(schema: String): String = {
    val tableName = getStagingTableName()
    s"""
       |FROM ${schema}_stage.$tableName staging
       | JOIN ${schema}_stage.rel_user u ON u.user_id = staging.staff_user_id
       |""".stripMargin
  }

  private def makeColNames(cols: List[String]): String =
    cols
      .map {
        case "staff_user_dw_id"  => s"\tu.user_dw_id AS staff_user_dw_id"
        case col => s"\t$col"
      }.mkString(",\n")
}
