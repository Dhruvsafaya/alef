package com.alefeducation.warehouse.tutor

import com.alefeducation.warehouse.core.CommonTransformer

object TutorAnalogousTransformer extends CommonTransformer {
  private val mainColNames: List[String] = List(
    "fta_dw_id",
    "fta_created_time",
    "fta_dw_created_time",
    "fta_date_dw_id",
    "fta_user_id",
    "fta_user_dw_id",
    "fta_tenant_id",
    "fta_message_id",
    "fta_session_id",
    "fta_conversation_id"
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
       |INSERT INTO $schema.fact_tutor_analogous ($insCols)
       |SELECT $selCols
       |${fromStatement(schema)}
       |WHERE stcqa.$getPkColumn IN ($idsInStatement)
       |""".stripMargin
  }

  override def getPkColumn(): String = "fta_dw_id"

  override def getStagingTableName(): String = "staging_tutor_analogous"

  private def makeColNames(cols: List[String]): String =
    cols
      .map {
        case "fta_user_dw_id"  => s"\tu.user_dw_id AS fta_user_dw_id"
        case col => s"\t$col"
      }.mkString(",\n")

  private def fromStatement(schema: String): String =
    s"""
       |FROM ${schema}_stage.staging_tutor_analogous stcqa
       |JOIN ${schema}_stage.rel_user u ON u.user_id = stcqa.fta_user_id
       |""".stripMargin
}