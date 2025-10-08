package com.alefeducation.warehouse.tutor

import com.alefeducation.warehouse.core.CommonTransformer

object TutorTranslationTransformer extends CommonTransformer {
  private val mainColNames: List[String] = List(
    "ftt_dw_id",
    "ftt_created_time",
    "ftt_dw_created_time",
    "ftt_date_dw_id",
    "ftt_user_id",
    "ftt_user_dw_id",
    "ftt_tenant_id",
    "ftt_message_id",
    "ftt_session_id",
    "ftt_conversation_id",
    "ftt_translation_language"
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
       |INSERT INTO $schema.fact_tutor_translation ($insCols)
       |SELECT $selCols
       |${fromStatement(schema)}
       |WHERE stcqa.$getPkColumn IN ($idsInStatement)
       |""".stripMargin
  }

  override def getPkColumn(): String = "ftt_dw_id"

  override def getStagingTableName(): String = "staging_tutor_translation"

  private def makeColNames(cols: List[String]): String =
    cols
      .map {
        case "ftt_user_dw_id"  => s"\tu.user_dw_id AS ftt_user_dw_id"
        case col => s"\t$col"
      }.mkString(",\n")

  private def fromStatement(schema: String): String =
    s"""
       |FROM ${schema}_stage.staging_tutor_translation stcqa
       |JOIN ${schema}_stage.rel_user u ON u.user_id = stcqa.ftt_user_id
       |""".stripMargin
}
