package com.alefeducation.warehouse.school
import com.alefeducation.warehouse.core.CommonTransformer

object SchoolContentRepositoryAssociationTransformer  extends CommonTransformer {

  private val mainColNames: List[String] = List(
    "scra_dw_id",
    "scra_school_id",
    "scra_school_dw_id",
    "scra_content_repository_id",
    "scra_content_repository_dw_id",
    "scra_status",
    "scra_active_until",
    "scra_created_time",
    "scra_dw_created_time",
  )

  override def getSelectQuery(schema: String): String = {
    val schoolContentRepositoryAssociationPkCol = getPkColumn()
    val from = fromStatement(schema)

    s"SELECT $schoolContentRepositoryAssociationPkCol $from ORDER BY $schoolContentRepositoryAssociationPkCol LIMIT $QUERY_LIMIT"
  }

  override def getInsertFromSelectQuery(schema: String, ids: List[Long]): String = {
    val idInStatement = ids.mkString(",")
    val insCols = mainColNames.mkString("\n\t", ",\n\t", "\n")
    val selCols = makeColNames(mainColNames)
    s"""
       |INSERT INTO $schema.dim_school_content_repository_association ($insCols)
       |SELECT $selCols
       |${fromStatement(schema)}
       |WHERE staging.$getPkColumn IN ($idInStatement)
       |""".stripMargin
  }

  override def getPkColumn(): String = "scra_dw_id"

  override def getStagingTableName(): String = "rel_school_content_repository_association"

  private def fromStatement(schema: String): String = {
    val tableName = getStagingTableName()
    s"""
       |FROM ${schema}_stage.$tableName staging
       | JOIN $schema.dim_school s ON s.school_id = staging.scra_school_id
       | JOIN $schema.dim_content_repository r ON r.content_repository_id = staging.scra_content_repository_id
       |""".stripMargin
  }

  private def makeColNames(cols: List[String]): String =
    cols
      .map {
        case "scra_school_dw_id"  => s"\ts.school_dw_id AS scra_school_dw_id"
        case "scra_content_repository_dw_id"  => s"\tr.content_repository_dw_id AS scra_content_repository_dw_id"
        case col => s"\t$col"
      }.mkString(",\n")
}
