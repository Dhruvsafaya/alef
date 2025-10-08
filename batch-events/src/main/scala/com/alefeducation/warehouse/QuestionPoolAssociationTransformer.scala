package com.alefeducation.warehouse
import com.alefeducation.warehouse.core.CommonTransformer

object QuestionPoolAssociationTransformer extends CommonTransformer  {

  val mainColNames: List[String] = List(
    "question_pool_association_created_time",
    "question_pool_association_updated_time",
    "question_pool_association_dw_created_time",
    "question_pool_association_dw_updated_time",
    "question_pool_association_status",
    "question_pool_association_assign_status",
    "question_pool_association_question_code",
    "question_pool_association_question_pool_dw_id",
    "question_pool_association_triggered_by"
  )

  private def makeColumnNames(cols: List[String]): String = cols.map {
    case "question_pool_association_question_pool_dw_id" =>
      s"dqp.question_pool_dw_id as question_pool_association_question_pool_dw_id"
    case "question_pool_association_dw_created_time" => "current_timestamp as question_pool_association_dw_created_time"
    case "question_pool_association_dw_updated_time" => "null as question_pool_association_dw_updated_time"
    case col => col
  }.mkString(", ")

  override def getSelectQuery(schema: String): String = {
    val questionPoolAssociationPk = getPkColumn()
    val from = fromStatement(schema)

    s"SELECT $questionPoolAssociationPk $from ORDER BY $questionPoolAssociationPk LIMIT $QUERY_LIMIT"
  }

  override def getInsertFromSelectQuery(schema: String, ids: List[Long]): String = {
    val idsInStatement = ids.mkString(",")
    val insCols = mainColNames.mkString("\n\t", ",\n\t", "\n")
    val selCols = makeColumnNames(mainColNames)
    s"""
       |INSERT INTO $schema.dim_question_pool_association ($insCols)
       |SELECT $selCols
       |${fromStatement(schema)}
       |WHERE staging.$getPkColumn IN ($idsInStatement)
       |""".stripMargin
  }

  override def getPkColumn(): String = "rel_question_pool_association_id"

  override def getStagingTableName(): String = "rel_question_pool_association"

  private def fromStatement(schema: String): String = {
    val tableName = getStagingTableName()
    s"""
       |FROM ${schema}_stage.$tableName staging
       |  INNER JOIN $schema.dim_question_pool dqp ON staging.question_pool_uuid = dqp.question_pool_id
       |""".stripMargin
  }
}
