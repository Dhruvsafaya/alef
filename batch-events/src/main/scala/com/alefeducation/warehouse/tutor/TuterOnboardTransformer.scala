package com.alefeducation.warehouse.tutor

import com.alefeducation.warehouse.core.CommonTransformer

object TuterOnboardTransformer extends CommonTransformer {
    private val mainColNames: List[String] = List(
        "fto_dw_id",
        "fto_created_time",
        "fto_dw_created_time",
        "fto_date_dw_id",
        "fto_user_id",
        "fto_tenant_id",
        "fto_question_id",
        "fto_question_category",
        "fto_user_free_text_response",
        "fto_onboarding_complete",
        "fto_onboarding_skipped",
        "fto_user_dw_id"
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
         |INSERT INTO $schema.fact_tutor_onboarding ($insCols)
         |SELECT $selCols
         |${fromStatement(schema)}
         |WHERE stcqa.$getPkColumn IN ($idsInStatement)
         |""".stripMargin
    }

    override def getPkColumn(): String = "fto_dw_id"

    override def getStagingTableName(): String = "staging_tutor_onboarding"

    private def makeColNames(cols: List[String]): String =
      cols
        .map {
          case "fto_user_dw_id"  => s"\tu.user_dw_id AS fto_user_dw_id"
          case col => s"\t$col"
        }.mkString(",\n")

    private def fromStatement(schema: String): String =
      s"""
         |FROM ${schema}_stage.staging_tutor_onboarding stcqa
         |JOIN ${schema}_stage.rel_user u ON u.user_id = stcqa.fto_user_id
         |""".stripMargin

}
