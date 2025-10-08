package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.CommonRedshiftTransformer
import com.alefeducation.warehouse.models.WarehouseConnection

object CertificateAwardedTransformer extends CommonRedshiftTransformer {

  override val mainTableName: String = "fact_student_certificate_awarded"

  override val stagingTableName: String = "staging_student_certificate_awarded"
  override val pkCol: List[String] = List("fsca_dw_id")
  override def pkNotation: Map[String, String] = Map(stagingTableName -> "fsca_dw_id")

  override val mainColumnNames: List[String] = List(
    "fsca_created_time",
    "fsca_dw_created_time",
    "fsca_date_dw_id",
    "fsca_tenant_dw_id",
    "fsca_certificate_id",
    "fsca_student_dw_id",
    "fsca_academic_year_dw_id",
    "fsca_grade_dw_id",
    "fsca_class_dw_id",
    "fsca_teacher_dw_id",
    "fsca_award_category",
    "fsca_award_purpose",
    "fsca_language"
  )

  override def getSelectQuery(cols: List[String], connection: WarehouseConnection): String =
    s"""
      |SELECT
      |   ${makeColumnNames(cols)}
      |FROM ${connection.schema}_stage.$stagingTableName staging
      | INNER JOIN ${connection.schema}.dim_tenant t ON staging.fsca_tenant_id = t.tenant_id
      | INNER JOIN ${connection.schema}_stage.rel_user s ON staging.fsca_student_id = s.user_id
      | LEFT JOIN ${connection.schema}.dim_academic_year day ON staging.fsca_academic_year_id = day.academic_year_id
      |   AND day.academic_year_status = 1 AND day.academic_year_is_roll_over_completed = false
      | INNER JOIN ${connection.schema}.dim_grade g ON staging.fsca_grade_id = g.grade_id
      | INNER JOIN ${connection.schema}_stage.rel_dw_id_mappings c ON staging.fsca_class_id = c.id
      |   AND entity_type = 'class'
      | INNER JOIN ${connection.schema}_stage.rel_user teacher ON staging.fsca_teacher_id = teacher.user_id
      |WHERE
      | (staging.fsca_academic_year_id IS NULL AND day.academic_year_dw_id IS NULL) OR
      | (staging.fsca_academic_year_id IS NOT NULL AND day.academic_year_dw_id IS NOT NULL)
      |ORDER BY ${pkNotation(stagingTableName)}
      |LIMIT $QUERY_LIMIT
      |""".stripMargin

  def makeColumnNames(cols: List[String]): String =
    cols
      .map {
        case "fsca_tenant_dw_id"           => s"\tt.tenant_dw_id AS fsca_tenant_dw_id"
        case "fsca_student_dw_id"          => s"\ts.user_dw_id AS fsca_student_dw_id"
        case "fsca_academic_year_dw_id"    => s"\tday.academic_year_dw_id AS fsca_academic_year_dw_id"
        case "fsca_grade_dw_id"            => s"\tg.grade_dw_id AS fsca_grade_dw_id"
        case "fsca_class_dw_id"            => s"\tc.dw_id AS fsca_class_dw_id"
        case "fsca_teacher_dw_id"          => s"\tteacher.user_dw_id AS fsca_teacher_dw_id"
        case col                           => s"\t$col"
      }
      .mkString(",\n")
}
