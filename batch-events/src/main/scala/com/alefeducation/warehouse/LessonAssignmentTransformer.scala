package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.RedshiftTransformer
import scalikejdbc.{AutoSession, SQL}
import com.alefeducation.warehouse.models.{QueryMeta, WarehouseConnection}

object LessonAssignmentTransformer extends RedshiftTransformer {

  override def tableNotation: Map[String, String] = Map.empty

  override def columnNotation: Map[String, String] = Map.empty

  override def pkNotation = Map(
    "rel_lesson_assignment" -> "rel_lesson_assignment_id"
  )

  val dwIds = List(
  "lesson_assignment_teacher_dw_id",
  "lesson_assignment_student_dw_id",
  "lesson_assignment_lo_dw_id",
  "lesson_assignment_class_dw_id"
  )

  val uuids = List(
    "teacher.user_dw_id as lesson_assignment_teacher_dw_id",
    "student.user_dw_id as lesson_assignment_student_dw_id",
    "CASE WHEN ic.ic_dw_id is not NULL " +
      "THEN ic.ic_dw_id " +
      "ELSE lo.lo_dw_id END AS lesson_assignment_lo_dw_id",
    "c.dw_id as lesson_assignment_class_dw_id"
  )

  override def prepareQueries(connection: WarehouseConnection)(implicit session: AutoSession): List[QueryMeta] = {
    val table = pkNotation.keys.headOption.getOrElse("").replace("rel", "dim")
    val tableColumns =
      SQL(
        s"select column_name from information_schema.columns where " +
          s"table_schema = '${connection.schema}' and table_name = '$table' and " +
          s"ordinal_position != 1 order by ordinal_position;")
        .map(_.string("column_name"))
        .list
        .apply

    val dimTableColumns = tableColumns.diff(dwIds)
    val pkSelectQuery = getSelectQuery(List(pkNotation("rel_lesson_assignment")), connection)
    val selectStatement = getSelectQuery(dimTableColumns ++ uuids, connection)

    val insertStatement =
      s"""
         |
         |INSERT INTO ${connection.schema}.dim_lesson_assignment (${(dimTableColumns ++ dwIds).mkString(", ")})
         | (
         |  ${selectStatement}
         | )
         |""".stripMargin

    log.info(s"pkSelectQuery :  ${pkSelectQuery}")
    log.info(s"insertQuery : ${insertStatement}")

    List(
      QueryMeta(
        "rel_lesson_assignment",
        pkSelectQuery,
        insertStatement
      )
    )
  }

  def getSelectQuery(cols: List[String], connection: WarehouseConnection): String = {
    s"""
       |select
       |     ${cols.mkString(",")}
       |from ${connection.schema}_stage.rel_lesson_assignment a
       |inner join ${connection.schema}_stage.rel_user teacher on teacher.user_id = a.teacher_uuid
       |inner join ${connection.schema}_stage.rel_user student on student.user_id = a.student_uuid
       |left join ${connection.schema}.dim_learning_objective lo  on lo.lo_id = a.lo_uuid
       |left join ${connection.schema}.dim_interim_checkpoint ic on ic.ic_id = a.lo_uuid
       |left join ${connection.schema}_stage.rel_dw_id_mappings c on a.class_uuid = c.id and c.entity_type = 'class'
       |where
       |((a.class_uuid isnull and c.dw_id isnull) or (a.class_uuid notnull and c.dw_id notnull)) and
       |(lo.lo_id is not null or ic.ic_id is not null)
       |order by ${pkNotation("rel_lesson_assignment")}
       |limit $QUERY_LIMIT
       |""".stripMargin
   }
}
