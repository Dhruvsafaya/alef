package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.RedshiftTransformer
import scalikejdbc.{AutoSession, SQL}
import com.alefeducation.warehouse.models.{QueryMeta, WarehouseConnection}

import scala.collection.immutable.ListMap

object RedshiftFactTransformer extends RedshiftTransformer {

  private val subjectAndSectionSet: Set[String] = Set("subject", "section", "learning_path")
  private val classSubjectAndSectionSet: Set[String] = subjectAndSectionSet + "class"

  private val stagingTablesWithLeftJoin: Set[String] = Set(
    "staging_practice",
    "staging_ktg",
    "staging_ktgskipped",
    "staging_star_awarded"
  )

  private val stagingColMapping: Map[String, String] = Map(
    "fasr_fle_ls_uuid" -> "fle_ls_uuid",
    "fanq_fle_ls_uuid" -> "fle_ls_uuid"
  )

  override def tableNotation: Map[String, String] =
    Map(
      "lo" -> "learning_objective",
      "learning_path" -> "learning_path",
      "curr_grade" -> "curriculum_grade",
      "curr_subject" -> "curriculum_subject"
    )

  override def columnNotation: Map[String, String] = ListMap(
    "fact_learning_experience" -> "fle"
  )

  override def pkNotation: ListMap[String, String] = ListMap(
    "staging_star_awarded" -> "fsa_staging_id",
    "staging_conversation_occurred" -> "fco_staging_id",
    "staging_ktg" -> "ktg_staging_id",
    "staging_ktgskipped" -> "ktgskipped_staging_id"
  )

  def factColumnPrefix: Map[String, String] = Map(
    "fact_star_awarded" -> "fsa",
    "fact_conversation_occurred" -> "fco",
    "fact_ktg" -> "ktg",
    "fact_ktgskipped" -> "ktgskipped"
  )

  def strippedColToTable: Map[String, String] = Map(
    "lp" -> "learning_path",
    "fle_ls" -> "fact_learning_experience",
    "exp" -> "fact_learning_experience",
    "ls" -> "fact_learning_experience"
  )

  def getColStripped(factPrefix: String, col: String): String = {
    (factPrefix, col) match {
      case ("practice", col1) if col1.contains("practice_item") => stripDwColumn(col, "practice_item")
      case _                                                    => stripDwColumn(col, factPrefix)
    }
  }

  override def prepareQueries(connection: WarehouseConnection)(implicit session: AutoSession): List[QueryMeta] = {
    val tables = pkNotation.keys.toList

    tables.map { stagingTable =>
      val factTable = stagingTable.replace("staging", "fact")
      val tableColumns =
        SQL(
          s"select column_name from information_schema.columns where table_schema = '${connection.schema}' and table_name = '$factTable' and ordinal_position != 1 order by ordinal_position;")
          .map(_.string("column_name"))
          .list
          .apply

      val factColumns = tableColumns.mkString(",\n\t")

      val factPrefix = factColumnPrefix(factTable)

      val stagingColumns = tableColumns
        .map(makeStagingColumns(factPrefix))
        .mkString(",\n\t")

      val termDwIds = tableColumns.filter(_.endsWith("term_dw_id"))

      val joinWithWhereCondition = (tableColumns.diff(termDwIds) ++ termDwIds)
        .filter(_.endsWith("_dw_id"))
        .filterNot(_.contains("date"))
        .map(makeJoin(connection, stagingTable, factPrefix))
        .sortWith((current, _) => current._1.contains("fact"))

      val whereConditionList = joinWithWhereCondition.filter(_._2.nonEmpty).map(_._2)
      val whereConditionString = if (whereConditionList.isEmpty) "" else whereConditionList.mkString("\nwhere", " and ", "")
      val joins = joinWithWhereCondition.map(_._1).mkString("\n") + whereConditionString

      val selectQuery =
        s"""
           |( select $stagingColumns
           | from ${connection.schema}_stage.$stagingTable
           | $joins order by ${getPkColumn(stagingTable)} limit $QUERY_LIMIT);
     """.stripMargin

      val pkSelectQuery =
        s"""
           |( select ${getPkColumn(stagingTable)}
           | from ${connection.schema}_stage.$stagingTable
           | $joins order by ${getPkColumn(stagingTable)} limit $QUERY_LIMIT);
     """.stripMargin

      val insertQuery =
        s"""
           | insert into ${connection.schema}.$factTable
           | ( $factColumns )
           | $selectQuery
  """.stripMargin

      log.info(s"Query Prepared for $stagingTable is : $insertQuery")
      println(s"Query Prepared for $stagingTable is : $insertQuery")
      QueryMeta(stagingTable, pkSelectQuery, insertQuery)
    }

  }

  def makeStagingColumns(factPrefix: String)(col: String): String = {
    if (!col.endsWith("dw_id") || col.contains("date")) stagingColMapping.getOrElse(col, col)
    else {
      val colStripped = getColStripped(factPrefix, col)
      val name = strippedColToTable.getOrElse(colStripped, colStripped)
      val tableName = getTable(name)
      val columnName = getColumn(name)
      val tableWithPrefix = Option(tableName).filter(_.startsWith("fact")).getOrElse(s"dim_$tableName")

      if (isUser(colStripped)) s"${colStripped}1.user_dw_id"
      else if (col.contains("item")) s"${colStripped}1.${getColumn(colStripped)}_dw_id"
      else s"$tableWithPrefix.${columnName}_dw_id"
    }
  }

  private def makeJoin(connection: WarehouseConnection, stagingTable: String, factPrefix: String)(column: String): (String, String) = {
    val x = getColStripped(factPrefix, column)
    val name = strippedColToTable.getOrElse(x, x)
    val tableName = getTable(name)
    val columnName = getColumn(name)
    val tableWithPrefix = Option(tableName).filter(_.startsWith("fact")).getOrElse(s"dim_$tableName")

    val joinType: String = makeJoinType(stagingTable, tableName)
    val join = if (isUser(x)) {
      s"inner join ${connection.schema}_stage.rel_user  ${x}1 on ${x}1.user_id = $stagingTable.${x}_uuid"
    } else if (column.contains("item")) {
      s"inner join ${connection.schema}.$tableWithPrefix  ${x}1 on ${x}1.${x}_id = $stagingTable.item_${x}_uuid"
    } else if (tableWithPrefix == "dim_class") {
      s"$joinType join (select distinct class_dw_id, class_id from ${connection.schema}.dim_class) dim_class on dim_class.class_id = $stagingTable.${x}_uuid "
    } else if(tableWithPrefix == "dim_role") {
      s"$joinType join ${connection.schema}.$tableWithPrefix on $tableWithPrefix.${columnName}_name = $stagingTable.${x}_uuid"
    }
    else {
      val optionalJoinCondition = makeOptionalJoinCondition(tableWithPrefix, columnName)
      s"$joinType join ${connection.schema}.$tableWithPrefix on $tableWithPrefix.${columnName}_id = $stagingTable.${x}_uuid $optionalJoinCondition"
    }

    val whereCondition = makeWhereCondition(stagingTable, tableName)

    (join, whereCondition)
  }

  def makeOptionalJoinCondition(tableWithPrefix: String, columnName: String): String = {
    if (Set("dim_curriculum_subject", "dim_academic_year").contains(tableWithPrefix)) {
      s"and $tableWithPrefix.${columnName}_status = 1"
    } else ""
  }

  def makeJoinType(stagingTable: String, tableName: String): String = {
    if (
      stagingTablesWithLeftJoin.contains(stagingTable) && classSubjectAndSectionSet.contains(tableName) ||
      stagingTable == "staging_conversation_occurred" && tableName == "subject" ||
      Set("staging_practice", "staging_ktg", "staging_ktgskipped", "staging_star_awarded").contains(stagingTable) && tableName == "academic_year"
    ) {
      "left"
    } else "inner"
  }

  def makeWhereCondition(stagingTable: String, table: String): String =
    if (stagingTablesWithLeftJoin.contains(stagingTable) && classSubjectAndSectionSet.contains(table) ||
        stagingTable == "staging_conversation_occurred" && table == "subject" ||
        Set("staging_practice", "staging_ktg", "staging_ktgskipped", "staging_star_awarded").contains(stagingTable) && table == "academic_year") {
      s"\n ((nvl(${table}_uuid, '') = '' and dim_$table.${table}_dw_id isnull) or (nvl(${table}_uuid, '') <> '' and dim_$table.${table}_dw_id notnull)) "
    } else ""

}
