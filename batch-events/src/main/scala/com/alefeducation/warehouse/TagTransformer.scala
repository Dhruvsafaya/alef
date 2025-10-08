package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.CommonRedshiftTransformer
import com.alefeducation.warehouse.models.WarehouseConnection

object TagTransformer extends CommonRedshiftTransformer {

  override val pkCol: List[String] = List("tag_dw_id")
  override val mainColumnNames: List[String] = List(
    "tag_dw_id",
    "tag_created_time",
    "tag_updated_time",
    "tag_dw_created_time",
    "tag_dw_updated_time",
    "tag_id",
    "tag_name",
    "tag_status",
    "tag_type",
    "tag_association_id",
    "tag_association_dw_id",
    "tag_association_type",
    "tag_association_attach_status"
  )
  override val stagingTableName: String = "rel_tag"
  override val mainTableName: String = "dim_tag"

  override def getSelectQuery(cols: List[String], connection: WarehouseConnection): String = {
    s"""
       |SELECT
       |   ${makeColumnNames(cols)}
       |FROM ${connection.schema}_stage.$stagingTableName staging
       |LEFT JOIN ${connection.schema}.dim_school s ON staging.tag_association_id = s.school_id AND tag_type = 'SCHOOL'
       |LEFT JOIN ${connection.schema}.dim_grade g ON staging.tag_association_id = g.grade_id AND tag_type = 'GRADE'
       |WHERE s.school_dw_id IS NOT NULL or g.grade_dw_id IS NOT NULL
       |ORDER BY ${pkNotation(stagingTableName)}
       |LIMIT $QUERY_LIMIT
       |""".stripMargin
  }

    def makeColumnNames(cols: List[String]): String =
      cols
        .map {
          case "tag_association_dw_id" => s"\tnvl(s.school_dw_id, g.grade_dw_id) AS tag_association_dw_id"
          case col                     => s"\t$col"
        }
        .mkString(",\n")

  override def pkNotation: Map[String, String] = Map(stagingTableName -> "tag_dw_id")
}
