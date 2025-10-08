package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.RedshiftTransformer
import scalikejdbc.AutoSession
import com.alefeducation.warehouse.models.{QueryMeta, WarehouseConnection}

object AssignmentSubmissionTransformer extends RedshiftTransformer {

  override def tableNotation: Map[String, String] = Map.empty

  override def columnNotation: Map[String, String] = Map.empty

  override def pkNotation = Map(
    "staging_assignment_submission" -> "assignment_submission_staging_id"
  )

  override def prepareQueries(connection: WarehouseConnection)(implicit session: AutoSession): List[QueryMeta] = {

    val pkSelectQuery = getSelectQuery(pkCol, connection)

    val selectStatement = getSelectQuery(factTableColumns ++ uuids, connection)

    val insertStatement =
      s"""
         |
         |INSERT INTO ${connection.schema}.fact_assignment_submission (${(factTableColumns ++ dw_ids).mkString(", ")})
         | (
         |  ${selectStatement}
         | )
         |""".stripMargin

    log.info(s"pkSelectQuery :  ${pkSelectQuery}")
    log.info(s"insertQuery : ${insertStatement}")

    List(
      QueryMeta(
        "staging_assignment_submission",
        pkSelectQuery,
        insertStatement
      )
    )
  }

  val pkCol = List(
    "assignment_submission_staging_id"
  )

  val factTableColumns = List(
    "assignment_submission_id",
    "assignment_submission_assignment_id",
    "assignment_submission_referrer_id",
    "assignment_submission_type",
    "assignment_submission_updated_on",
    "assignment_submission_returned_on",
    "assignment_submission_submitted_on",
    "assignment_submission_graded_on",
    "assignment_submission_evaluated_on",
    "assignment_submission_status",
    "assignment_submission_student_attachment_file_name",
    "assignment_submission_student_attachment_path",
    "assignment_submission_teacher_attachment_path",
    "assignment_submission_teacher_attachment_file_name",
    "assignment_submission_teacher_score",
    "assignment_submission_date_dw_id",
    "assignment_submission_created_time",
    "assignment_submission_dw_created_time",
    "assignment_submission_has_teacher_comment",
    "assignment_submission_has_student_comment",
    "assignment_submission_assignment_instance_id",
    "assignment_submission_resubmission_count",
    "eventdate"
  )

  val dw_ids = List(
    "assignment_submission_student_dw_id",
    "assignment_submission_teacher_dw_id",
    "assignment_submission_tenant_dw_id"
  )

  val uuids = List(
    "student.user_dw_id as assignment_submission_student_dw_id",
    "teacher.user_dw_id as assignment_submission_teacher_dw_id",
    "t.tenant_dw_id as assignment_submission_tenant_dw_id"
  )

  def getSelectQuery(cols: List[String], connection: WarehouseConnection): String = {

    s"""
       |select
       |     ${cols.mkString(",")}
       |from ${connection.schema}_stage.staging_assignment_submission a
       |inner join ${connection.schema}.dim_tenant t on t.tenant_id = a.assignment_submission_tenant_id
       |inner join ${connection.schema}_stage.rel_user  student on student.user_id = a.assignment_submission_student_id
       |left join ${connection.schema}_stage.rel_user  teacher on teacher.user_id = a.assignment_submission_teacher_id
       |
       |where (a.assignment_submission_teacher_id is null) or (teacher.user_id is not null)
       |order by ${pkCol.mkString(",")}
       |limit $QUERY_LIMIT
       |""".stripMargin

  }

}
