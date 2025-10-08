package com.alefeducation.warehouse.staff

import com.alefeducation.warehouse.core.CommonTransformer

object StaffUserSchoolRoleAssociationTransformer extends CommonTransformer {

  private val mainColNames: List[String] = List(
    "susra_dw_id",
    "susra_staff_id",
    "susra_staff_dw_id",
    "susra_school_id",
    "susra_school_dw_id",
    "susra_role_name",
    "susra_role_uuid",
    "susra_role_dw_id",
    "susra_organization",
    "susra_status",
    "susra_created_time",
    "susra_active_until",
    "susra_dw_created_time",
    "susra_event_type"
  )

  override def getSelectQuery(schema: String): String = {
    val staffUserSchoolRoleAssociationPkCol = getPkColumn()
    val from = fromStatement(schema)

    s"SELECT $staffUserSchoolRoleAssociationPkCol $from ORDER BY $staffUserSchoolRoleAssociationPkCol LIMIT $QUERY_LIMIT"
  }

  override def getInsertFromSelectQuery(schema: String, ids: List[Long]): String = {
    val idInStatement = ids.mkString(",")
    val insCols = mainColNames.mkString("\n\t", ",\n\t", "\n")
    val selCols = makeColNames(mainColNames)
    s"""
       |INSERT INTO $schema.dim_staff_user_school_role_association ($insCols)
       |SELECT $selCols
       |${fromStatement(schema)}
       |AND staging.$getPkColumn IN ($idInStatement)
       |""".stripMargin
  }

  override def getPkColumn(): String = "susra_dw_id"

  override def getStagingTableName(): String = "rel_staff_user_school_role_association"

  private def fromStatement(schema: String): String = {
    val tableName = getStagingTableName()
    s"""
       |FROM ${schema}_stage.$tableName staging
       | JOIN ${schema}_stage.rel_user u ON u.user_id = staging.susra_staff_id
       | LEFT OUTER JOIN $schema.dim_school s ON s.school_id = staging.susra_school_id
       | JOIN $schema.dim_role r ON r.role_uuid = staging.susra_role_uuid
       |WHERE (staging.susra_school_id IS NULL OR s.school_id IS NOT NULL)
       |""".stripMargin
  }

  private def makeColNames(cols: List[String]): String =
    cols
      .map {
        case "susra_staff_dw_id"  => s"\tu.user_dw_id AS susra_staff_dw_id"
        case "susra_school_dw_id"  => s"\ts.school_dw_id AS susra_school_dw_id"
        case "susra_role_dw_id"  => s"\tr.role_dw_id AS susra_role_dw_id"
        case col => s"\t$col"
      }.mkString(",\n")
}
