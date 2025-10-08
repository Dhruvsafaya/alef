package com.alefeducation.warehouse
import com.alefeducation.warehouse.core.RedshiftTransformer
import scalikejdbc.AutoSession
import com.alefeducation.warehouse.models.{QueryMeta, WarehouseConnection}

object QuestionTransformer extends RedshiftTransformer {
  override def tableNotation: Map[String, String] = Map.empty

  override def columnNotation: Map[String, String] = Map.empty

  override def pkNotation: Map[String, String] = Map(
    "rel_question" -> "rel_question_id"
  )

  val pkCol = List(
    "rel_question_id"
  )

  val dimColumnNames = List(
    "question_dw_id",
    "question_created_time",
    "question_updated_time",
    "question_deleted_time",
    "question_dw_created_time",
    "question_dw_updated_time",
    "question_status",
    "question_id",
    "question_code",
    "question_triggered_by",
    "question_language",
    "question_type",
    "question_max_score",
    "question_version",
    "question_stage",
    "question_variant",
    "question_active_until",
    "question_format_type",
    "question_resource_type",
    "question_summative_assessment",
    "question_difficulty_level",
    "question_knowledge_dimensions",
    "question_lexile_level",
    "question_author",
    "question_authored_date",
    "question_skill_id",
    "question_cefr_level",
    "question_proficiency"
  )

  override def prepareQueries(connection: WarehouseConnection)(implicit session: AutoSession): List[QueryMeta] = {
    val selectPkQuery = getSelectQuery(pkCol, connection)
    val selectQuery = getSelectQuery(dimColumnNames, connection)
    val insertStatement =
      s"""
         |
         |INSERT INTO ${connection.schema}.dim_question (${dimColumnNames.mkString(", ")})
         | (
         |  $selectQuery
         | )
         |""".stripMargin
    log.info(s"pkSelectQuery :  $selectPkQuery")
    log.info(s"insertQuery : $insertStatement")
    List(QueryMeta("rel_question", selectPkQuery, insertStatement))
  }

  private def getSelectQuery(cols: List[String], connection: WarehouseConnection): String = {
    s"""
       |select
       |     ${makeColumnNames(cols)}
       |from ${connection.schema}_stage.rel_question rc
       |inner join ${connection.schema}_stage.rel_dw_id_mappings rdim on rc.question_id = rdim.id
       |where rdim.entity_type = 'question'
       |order by ${pkCol.mkString(",")}
       |      limit $QUERY_LIMIT
       |""".stripMargin
  }

  private def makeColumnNames(cols: List[String]): String = cols.map {
    case "question_dw_id" => s"dw_id as question_dw_id"
    case col => col
  }.mkString(", ")

}
