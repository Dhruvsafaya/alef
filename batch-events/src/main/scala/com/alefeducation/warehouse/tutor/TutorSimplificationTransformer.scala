package com.alefeducation.warehouse.tutor

import com.alefeducation.warehouse.core.CommonTransformer

object TutorSimplificationTransformer extends CommonTransformer {
  private val mainColNames: List[String] = List(
    "fts_dw_id",
    "fts_created_time",
    "fts_dw_created_time",
    "fts_date_dw_id",
    "fts_user_id",
    "fts_user_dw_id",
    "fts_tenant_id",
    "fts_message_id",
    "fts_session_id",
    "fts_conversation_id"
  )
  override def getSelectQuery(schema: String): String =  s"""
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
       |INSERT INTO $schema.fact_tutor_simplification ($insCols)
       |SELECT $selCols
       |${fromStatement(schema)}
       |WHERE stcqa.$getPkColumn IN ($idsInStatement)
       |""".stripMargin
  }

  override def getPkColumn(): String = "fts_dw_id"

  override def getStagingTableName(): String = "staging_tutor_simplification"

  private def makeColNames(cols: List[String]): String =
    cols
      .map {
        case "fts_user_dw_id"  => s"\tu.user_dw_id AS fts_user_dw_id"
        case col => s"\t$col"
      }.mkString(",\n")

  private def fromStatement(schema: String): String =
    s"""
       |FROM ${schema}_stage.staging_tutor_simplification stcqa
       |JOIN ${schema}_stage.rel_user u ON u.user_id = stcqa.fts_user_id
       |""".stripMargin
}
