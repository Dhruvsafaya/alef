package com.alefeducation.warehouse

import com.alefeducation.warehouse.RedshiftFactTransformer._
import com.alefeducation.warehouse.core.RedshiftTransformer
import com.alefeducation.warehouse.models.{QueryMeta, WarehouseConnection}
import scalikejdbc.{AutoSession, SQL}

object FactPracticeTransformer extends RedshiftTransformer {
  override def tableNotation: Map[String, String] = Map("lo" -> "learning_objective")

  override def columnNotation: Map[String, String] = Map()

  override def pkNotation: Map[String, String] = Map("staging_practice" -> "practice_staging_id")

  override def prepareQueries(connection: WarehouseConnection)(implicit session: AutoSession): List[QueryMeta] = {
    val factTable = "fact_practice"
    val stagingTable = "staging_practice"
    val tableColumns =
      SQL(
        s"select column_name from information_schema.columns where table_schema = '${connection.schema}' and table_name = '$factTable' and ordinal_position != 1 order by ordinal_position;")
        .map(_.string("column_name"))
        .list
        .apply

    val factColumns = tableColumns.mkString(",\n\t")

    val factPrefix = "practice"

    val stagingColumns = tableColumns.map(makeStagingColumns(factPrefix, ""))
    val stagingColumnsForCCl = tableColumns.map(makeStagingColumns(factPrefix, "ccl"))

    val joinsWithWhere: String = getJoinWithWhere(connection, stagingTable, tableColumns, factPrefix, "")
    val joinsWithWhereForCclSkill: String = getJoinWithWhere(connection, stagingTable, tableColumns, factPrefix, "ccl")

    val selectQuery =
      s"""
         | select ${factColumns} from
         | ( select ${(stagingColumns :+ getPkColumn(stagingTable)).mkString(",\n\t")}
         | from ${connection.schema}_stage.$stagingTable
         | $joinsWithWhere
         | union all
         | select ${(stagingColumnsForCCl :+ getPkColumn(stagingTable)).mkString(",\n\t")}
         | from ${connection.schema}_stage.$stagingTable
         | $joinsWithWhereForCclSkill
         | order by ${getPkColumn(stagingTable)} limit $QUERY_LIMIT);
     """.stripMargin

    val pkSelectQuery =
      s"""
         |(select ${getPkColumn(stagingTable)}
         | from ${connection.schema}_stage.$stagingTable
         | $joinsWithWhere
         | union all
         | select ${getPkColumn(stagingTable)}
         | from ${connection.schema}_stage.$stagingTable
         | $joinsWithWhereForCclSkill
         | order by ${getPkColumn(stagingTable)} limit $QUERY_LIMIT);
     """.stripMargin

    val insertQuery =
      s"""
         | insert into ${connection.schema}.$factTable
         | ( $factColumns )
         | $selectQuery
  """.stripMargin

    log.info(s"Query Prepared for $stagingTable is : $insertQuery")
    println(s"Query Prepared for $stagingTable is : $insertQuery")
    println(s"Query Prepared for pk column $stagingTable is : $pkSelectQuery")
    List(QueryMeta(stagingTable, pkSelectQuery, insertQuery))
  }

  def getJoinWithWhere(connection: WarehouseConnection, stagingTable: String, tableColumns: List[String], factPrefix: String, columnPrefix: String) = {
    val joinWithWhereCondition = (tableColumns)
      .filter(_.endsWith("_dw_id"))
      .filterNot(_.contains("date"))
      .map(makeJoin(connection.schema, stagingTable, factPrefix, columnPrefix))
      .sortWith((current, _) => current._1.contains("fact"))

    val whereConditionList = joinWithWhereCondition.filter(_._2.nonEmpty).map(_._2)
    val whereConditionString = if (whereConditionList.isEmpty) "" else whereConditionList.mkString("\nwhere", " and ", "")
    val joinsWithWhere = joinWithWhereCondition.map(_._1).mkString("\n") + whereConditionString
    joinsWithWhere
  }

  def makeJoin(schema: String, stagingTable: String, factPrefix: String, columnPrefix: String)(column: String): (String, String) = {
    val joinColumnName = getStrippedColumnName(factPrefix, columnPrefix, column)
    val targetColumnName = getColStripped(factPrefix, column)
    val name = strippedColToTable.getOrElse(joinColumnName, joinColumnName)
    val tableName = getTable(name)
    val columnName = getColumn(name)
    val tableWithPrefix = Option(tableName).filter(_.startsWith("fact")).getOrElse(s"dim_$tableName")

    val joinType: String = makeJoinType(stagingTable, tableName)
    val join = if (column.contains("item")) {
      s"inner join ${schema}.$tableWithPrefix  ${joinColumnName}1 on ${joinColumnName}1.${joinColumnName}_id = $stagingTable.item_${targetColumnName}_uuid "
    } else if (isUser(joinColumnName)) {
      s"inner join ${schema}_stage.rel_user  ${joinColumnName}1 on ${joinColumnName}1.user_id = $stagingTable.${joinColumnName}_uuid "
    } else if (tableWithPrefix == "dim_class") {
      s"$joinType join (select distinct class_dw_id, class_id from ${schema}.dim_class) dim_class on dim_class.class_id = $stagingTable.${targetColumnName}_uuid "
    } else {
      s"$joinType join ${schema}.$tableWithPrefix on $tableWithPrefix.${columnName}_id = $stagingTable.${targetColumnName}_uuid "
    }

    val whereCondition = makeWhereCondition(stagingTable, tableName)

    (join, whereCondition)
  }

  def getStrippedColumnName(factPrefix: String, columnPrefix: String, column: String) = {
    if (columnPrefix.nonEmpty && column.contains("skill")) s"${columnPrefix}_${getColStripped(factPrefix, column)}" else getColStripped(factPrefix, column)
  }

  def makeStagingColumns(factPrefix: String, columnPrefix: String)(col: String): String = {
    if (col.contains("dw_created_time")) s"getdate() as $col"
    else if (!col.endsWith("dw_id") || col.contains("date")) col
    else {
      val colStripped = getStrippedColumnName(factPrefix, columnPrefix, col)
      val name = strippedColToTable.getOrElse(colStripped, colStripped)
      val tableName = getTable(name)
      val columnName = getColumn(name)
      val tableWithPrefix = Option(tableName).filter(_.startsWith("fact")).getOrElse(s"dim_$tableName")

      if (isUser(colStripped)) s"${colStripped}1.user_dw_id as $col"
      else if (col.contains("item")) s"${colStripped}1.${getColumn(colStripped)}_dw_id as $col"
      else s"$tableWithPrefix.${columnName}_dw_id as $col"
    }
  }

}
