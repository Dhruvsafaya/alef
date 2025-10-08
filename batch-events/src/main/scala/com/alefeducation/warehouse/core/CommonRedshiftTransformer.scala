package com.alefeducation.warehouse.core

import com.alefeducation.warehouse.models.{QueryMeta, WarehouseConnection}
import scalikejdbc.AutoSession

@deprecated("extend CommonTransformer interface for impl. Transformer")
trait CommonRedshiftTransformer extends RedshiftTransformer {

  val pkCol : List[String]
  val mainColumnNames : List[String]
  val stagingTableName : String
  val mainTableName : String

  override def prepareQueries(connection: WarehouseConnection)(implicit session: AutoSession): List[QueryMeta] = {
    val selectPkQuery = getSelectQuery(pkCol, connection)
    val selectQuery = getSelectQuery(mainColumnNames, connection)
    val insertStatement =
      s"""
         |
         |INSERT INTO ${connection.schema}.$mainTableName
         | (${mainColumnNames.mkString(", \n")})
         | (
         |  $selectQuery
         | )
         |""".stripMargin
    log.info(s"pkSelectQuery :  $selectPkQuery")
    log.info(s"insertQuery : $insertStatement")
    List(QueryMeta(s"$stagingTableName", selectPkQuery, insertStatement))
  }

  def getSelectQuery(cols: List[String], connection: WarehouseConnection): String

}
