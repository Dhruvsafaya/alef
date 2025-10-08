package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.RedshiftTransformer
import scalikejdbc.AutoSession
import com.alefeducation.warehouse.models.{QueryMeta, WarehouseConnection}

object TeacherGuardianFeedbackDimensionTransformer extends RedshiftTransformer {

  override def tableNotation: Map[String, String] = Map.empty

  override def columnNotation: Map[String, String] = Map.empty

  override def pkNotation = Map("rel_teacher_feedback_thread" -> "rel_tft_id")

  val factTableName = "dim_teacher_feedback_thread"

  val pkCol = List("rel_tft_id")

  val tableColumns = List(
    "tft_status",
    "tft_created_time",
    "tft_dw_created_time",
    "tft_deleted_time",
    "tft_updated_time",
    "tft_dw_updated_time",
    "tft_thread_id",
    "tft_actor_type",
    "tft_message_id",
    "tft_response_enabled",
    "tft_feedback_type",
    "tft_is_read",
    "tft_event_subject",
    "tft_is_first_of_thread"
  )

  val dw_ids = List(
    "tft_teacher_dw_id",
    "tft_student_dw_id",
    "tft_class_dw_id",
    "tft_guardian_dw_id"
  )

  val uuids = List(
    "dt.user_dw_id as tft_teacher_dw_id",
    "ds.user_dw_id as tft_student_dw_id",
    "dc.dw_id as tft_class_dw_id",
    "dg.user_dw_id as tft_guardian_dw_id"
  )

  override def prepareQueries(connection: WarehouseConnection)(implicit session: AutoSession): List[QueryMeta] = {
    val pkSelectQuery = getSelectQuery(pkCol, connection)

    val selectQuery = getSelectQuery(tableColumns ++ uuids, connection)

    val insertStatement =
      s"""
         |
         |INSERT INTO ${connection.schema}.$factTableName (${(tableColumns ++ dw_ids).mkString(", ")})
         | (
         |  ${selectQuery}
         | )
         |""".stripMargin

    log.info(s"Prepare queries: $pkSelectQuery\n$insertStatement")
    List(
      QueryMeta(
        "rel_teacher_feedback_thread",
        pkSelectQuery,
        insertStatement
      )
    )
  }

  private def getSelectQuery(cols: List[String], connection: WarehouseConnection): String = {
    s"""
       |select
       |     ${cols.mkString(",")}
       |from ${connection.schema}_stage.rel_teacher_feedback_thread tft
       |left join ${connection.schema}_stage.rel_user dt on dt.user_id = tft.tft_teacher_id
       |left join ${connection.schema}_stage.rel_user ds on ds.user_id = tft.tft_student_id
       |left join ${connection.schema}_stage.rel_dw_id_mappings dc on dc.id = tft.tft_class_id and dc.entity_type = 'class'
       |left join ${connection.schema}_stage.rel_user dg on dg.user_id = tft.tft_guardian_id
       |where
       |  ((tft.tft_teacher_id is null) or (dt.user_id is not null)) and
       |  ((tft.tft_student_id is null) or (ds.user_id is not null)) and
       |  ((tft.tft_guardian_id is null) or (dg.user_id is not null)) and
       |  ((tft.tft_class_id is null) or (dc.id is not null))
       |order by ${pkCol.mkString(",")}
       |      limit $QUERY_LIMIT
       |""".stripMargin
  }

}
