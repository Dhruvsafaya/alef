package com.alefeducation.warehouse.core

import com.alefeducation.io.data.RedshiftIO.createMergeQuery
import com.alefeducation.warehouse.models.{QueryMeta, WarehouseConnection}
import scalikejdbc.AutoSession

trait UpsertRedshiftTransformer extends RedshiftTransformer {

  val primaryKeyColumns: List[String]
  val mainColumnNames: List[String]
  val stagingTableName: String
  val mainTableName: String
  val matchConditions: String
  val columnsToUpdate: Map[String, String]
  val columnsToInsert: Map[String, String]

  override def prepareQueries(connection: WarehouseConnection)(implicit session: AutoSession): List[QueryMeta] = {
    val temporaryTable: String = s"#${mainTableName}_temp"

    val mergeStatement: String = createMergeQuery(
      targetTable = s"${connection.schema}.$mainTableName",
      sourceTable = temporaryTable,
      matchConditions = matchConditions,
      columnsToUpdate = columnsToUpdate,
      columnsToInsert = columnsToInsert
    )

    val selectQuery: String = getSelectQuery(mainColumnNames, connection)

    val upsertStatement = s"""
       |BEGIN TRANSACTION;
       |
       |CREATE TEMPORARY TABLE $temporaryTable AS $selectQuery;
       |
       |$mergeStatement
       |
       |DROP TABLE $temporaryTable;
       |
       |END TRANSACTION;
       |""".stripMargin

    val selectPkQuery = getSelectQuery(primaryKeyColumns, connection)

    log.info(s"pkSelectQuery :  $selectPkQuery")
    log.info(s"upsertQuery : $upsertStatement")
    List(QueryMeta(stagingTableName, selectPkQuery, upsertStatement))
  }

  def getSelectQuery(cols: List[String], connection: WarehouseConnection): String

}
