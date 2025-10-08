package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.CommonTransformer

object TeacherTestCandidateSessionTransformer extends CommonTransformer {
  private val mainColNames: List[String] = List(
    "fttcp_dw_id",
    "fttcp_session_id",
    "fttcp_candidate_id",
    "fttcp_candidate_dw_id",
    "fttcp_test_delivery_id",
    "fttcp_assessment_id",
    "fttcp_test_delivery_dw_id",
    "fttcp_score",
    "fttcp_stars_awarded",
    "fttcp_status",
    "fttcp_updated_at",
    "fttcp_created_at",
    "fttcp_date_dw_id",
    "fttcp_created_time",
    "fttcp_dw_created_time"
  )

  override def getSelectQuery(schema: String): String = s"""
                                                           |SELECT
                                                           |     ${getPkColumn()}
                                                           |${fromStatement(schema)}
                                                           |ORDER BY ${getPkColumn()}
                                                           |LIMIT $QUERY_LIMIT
                                                           |""".stripMargin

  override def getInsertFromSelectQuery(schema: String, ids: List[Long]): String = {
    val idsInStatement = ids.mkString(",")
    log.info(idsInStatement)
    val insCols = mainColNames.mkString("\n\t", ",\n\t", "\n")
    val selCols = makeColNames(mainColNames)
    s"""
       |INSERT INTO $schema.fact_teacher_test_candidate_progress ($insCols)
       |SELECT $selCols
       |${fromStatement(schema)}
       |WHERE stcqa.$getPkColumn IN ($idsInStatement)
       |""".stripMargin
  }

  override def getPkColumn(): String = "fttcp_dw_id"

  override def getStagingTableName(): String = "staging_teacher_test_candidate_progress"

  private def makeColNames(cols: List[String]): String =
    cols
      .map {
        case "fttcp_test_delivery_dw_id"  => s"\td.ttds_dw_id AS fttcp_test_delivery_dw_id"
        case "fttcp_candidate_dw_id"  => s"\tu.user_dw_id AS fttcp_candidate_dw_id"
        case col => s"\t$col"
      }.mkString(",\n")

  private def fromStatement(schema: String): String =
    s"""
       |FROM ${schema}_stage.staging_teacher_test_candidate_progress stcqa
       |JOIN ${schema}_stage.rel_user u ON u.user_id = stcqa.fttcp_candidate_id
       |JOIN ${schema}.dim_teacher_test_delivery_settings d ON d.ttds_test_delivery_id = stcqa.fttcp_test_delivery_id
       |""".stripMargin
}
