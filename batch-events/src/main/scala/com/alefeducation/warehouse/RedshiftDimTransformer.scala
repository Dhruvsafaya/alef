package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.RedshiftTransformer
import scalikejdbc.{AutoSession, SQL}
import com.alefeducation.warehouse.models.{QueryMeta, WarehouseConnection}

object RedshiftDimTransformer extends RedshiftTransformer {

  override def tableNotation: Map[String, String] = Map(
    "dw_id" -> "user",
    "item_lo" -> "learning_objective",
    "item_week" -> "week",
    "lo" -> "learning_objective",
    "item_ic" -> "interim_checkpoint"
  )

  override def columnNotation: Map[String, String] = Map(
    "dw_id" -> "user",
    "item_lo" -> "lo",
    "item_week" -> "week",
    "item_ic" -> "ic"
  )

  override def pkNotation: Map[String, String] =
    Map(
      "rel_admin" -> "rel_admin_id",
      "rel_team_student_association" -> "rel_team_student_association_id"
    )

  override def prepareQueries(connection: WarehouseConnection)(implicit session: AutoSession): List[QueryMeta] = {
    val tables = pkNotation.keys.toList

    tables.map { relTable =>
      val dimTable = relTable.replace("rel", "dim")
      val entity = relTable.replace("rel_", "")
      val tableColumns =
        SQL(
          s"select column_name from information_schema.columns where table_schema = '${connection.schema}' and table_name = '$dimTable' and ordinal_position != 1 order by ordinal_position;")
          .map(_.string("column_name"))
          .list
          .apply

      val dimColumns = tableColumns.mkString(",\n\t")

      val stagingColumns = tableColumns.map(toStagingColumn(entity)).mkString(",\n\t")

      val joinWithWhereCondition: List[(String, String)] = tableColumns
        .filter(_.endsWith("_dw_id"))
        .filterNot(_.contains("date"))
        .map(stripDwColumn(_, entity))
        .map(col => (getJoins(connection, relTable)(col), getWhereConditions(connection, relTable)(col)))

      val joins = joinWithWhereCondition.map(_._1).mkString("\n") +
        joinWithWhereCondition.filter(_._2.nonEmpty).map(_._2).mkString("\n")

      val selectQuery =
        s"""
           |( select $stagingColumns
           | from ${connection.schema}_stage.$relTable
           | $joins order by ${getPkColumn(relTable)} limit $QUERY_LIMIT);
        """.stripMargin

      val pkSelectQuery =
        s"""
           |( select ${getPkColumn(relTable)},
           | $stagingColumns
           | from ${connection.schema}_stage.$relTable
           | $joins order by ${getPkColumn(relTable)} limit $QUERY_LIMIT);
        """.stripMargin

      val insertQuery =
        s"""
           | insert into ${connection.schema}.$dimTable
           | ( $dimColumns )
           | $selectQuery
        """.stripMargin

      log.info(s"Query Prepared for $relTable is : $insertQuery")
      QueryMeta(relTable, pkSelectQuery, insertQuery)
    }
  }

  private def getJoins(connection: WarehouseConnection, relTable: String)(col: String): String =
    if (col == "dw_id") {
      s"inner join ${connection.schema}_stage.rel_${getTable(col)} on rel_${getTable(col)}.${getColumn(col)}_id = $relTable.${stripDwColumn(relTable, "rel")}_uuid "
    } else if (isUser(col)) {
      val joinType = if (col == "student" && relTable == "rel_guardian") "left" else "inner"
      s"$joinType join ${connection.schema}_stage.rel_user alias_$col on alias_${col}.user_id = $relTable.${col}_uuid "
    } else if (isSCDDim(col)) {
      s"inner join ${connection.schema}_stage.rel_dw_id_mappings alias_$col on alias_${col}.id = $relTable.${col}_uuid and alias_${col}.entity_type='${col.replace("_", "-")}' "
    } else if (col == "role" && relTable == "rel_admin") {
      s"inner join ${connection.schema}.dim_${getTable(col)} on dim_${getTable(col)}.${getColumn(col)}_name = $relTable.${getColumn(col)}_uuid"
    } else {
      val joinType =
        if ((col == "subject" && relTable == "rel_teacher") || (col == "school" && relTable == "rel_admin")) "left" else "inner"
      s"$joinType join ${connection.schema}.dim_${getTable(col)} on dim_${getTable(col)}.${getColumn(col)}_id = $relTable.${getColumn(col)}_uuid ${checkStatusByCol(col)} ${if (relTable.equals("rel_guardian"))
        s" and ${getColumn(col)}_active_until is null"
      else ""} "
    }

  private def checkStatusByCol(col: String): String = {
    if (col == "class") s"and dim_class.class_status = 1"
    else ""
  }

  private def getWhereConditions(connection: WarehouseConnection, relTable: String)(col: String): String =
    if (col == "subject" && relTable == "rel_teacher") {
      s"where (subject_uuid isnull and dim_subject.subject_dw_id isnull) or (subject_uuid notnull and dim_subject.subject_dw_id notnull) "
    } else if (col == "student" && relTable == "rel_guardian") {
      s"where (student_uuid isnull and alias_student.user_dw_id isnull) or (student_uuid notnull and alias_student.user_dw_id notnull) "
    }
    else ""

  private def toStagingColumn(entity: String)(col: String): String = {
    if (col.endsWith("dw_id")) {
      val x = stripDwColumn(col, entity)
      val tablePrefix = if (x.equals("dw_id") || x.equals()) "rel" else "dim"

      if (entity.equals("guardian_invitation")) "rel_user.user_dw_id"
      else if (isUser(x)) s"alias_${x}.user_dw_id as ${getColumn(x)}_dw_id"
      else if (isSCDDim(x)) s"alias_${x}.dw_id as ${getColumn(x)}_dw_id"
      else s"${tablePrefix}_${getTable(x)}.${getColumn(x)}_dw_id"
    } else if (!entity.equals("instructional_plan") && col.endsWith("_id")) {
      col.replace("id", "uuid")
    } else col
  }
}
