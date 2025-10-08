package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.RedshiftTransformer
import scalikejdbc.AutoSession
import com.alefeducation.warehouse.models.{QueryMeta, WarehouseConnection}

object LearningExperienceTransformer extends RedshiftTransformer {

  override def tableNotation: Map[String, String] = Map.empty

  override def columnNotation: Map[String, String] = Map.empty

  override def pkNotation = Map(
    "staging_learning_experience" -> "fle_staging_id"
  )

  override def prepareQueries(connection: WarehouseConnection)(implicit session: AutoSession): List[QueryMeta] = {

    val pkSelectQuery = getSelectQuery(pkCol, connection)

    val selectStatement = getSelectQuery(factTableColumns.map{ case x if x.endsWith("dw_created_time") => "getdate()"; case x => x } ++ uuids, connection)

    val insertStatement =
      s"""
         |INSERT INTO ${connection.schema}.fact_learning_experience (${(factTableColumns ++ dw_ids).mkString(",\n\t")})
         | ($selectStatement
         |);""".stripMargin

    log.info(s"pkSelectQuery :  $pkSelectQuery")
    log.info(s"insertQuery : $insertStatement")

    List(
      QueryMeta(
        "staging_learning_experience",
        pkSelectQuery,
        insertStatement
      )
    )
  }

  val pkCol = List(
    "fle_staging_id"
  )

  val factTableColumns = List(
    "fle_created_time",
    "fle_dw_created_time",
    "fle_date_dw_id",
    "fle_exp_id",
    "fle_ls_id",
    "fle_start_time",
    "fle_end_time",
    "fle_total_time",
    "fle_score",
    "fle_star_earned",
    "fle_lesson_type",
    "fle_is_retry",
    "fle_outside_of_school",
    "fle_attempt",
    "fle_exp_ls_flag",
    "fle_academic_period_order",
    "fle_content_academic_year",
    "fle_time_spent_app",
    "fle_instructional_plan_id",
    "fle_lesson_category",
    "fle_adt_level",
    "fle_step_id",
    "fle_abbreviation",
    "fle_activity_template_id",
    "fle_activity_type",
    "fle_activity_component_type",
    "fle_exit_ticket",
    "fle_main_component",
    "fle_completion_node",
    "fle_total_score",
    "fle_is_activity_completed",
    "fle_material_id",
    "fle_material_type",
    "fle_state",
    "fle_open_path_enabled",
    "fle_source",
    "fle_teaching_period_id",
    "fle_academic_year",
    "fle_is_gamified",
    "fle_is_additional_resource",
    "fle_bonus_stars",
    "fle_bonus_stars_scheme"
  )

  val dw_ids = List(
    "fle_lo_dw_id",
    "fle_student_dw_id",
    "fle_subject_dw_id",
    "fle_grade_dw_id",
    "fle_class_dw_id",
    "fle_curr_subject_dw_id",
    "fle_curr_grade_dw_id",
    "fle_term_dw_id",
    "fle_tenant_dw_id",
    "fle_school_dw_id",
    "fle_section_dw_id",
    "fle_academic_year_dw_id"
  )

  val uuids = List(
    "CASE WHEN staging_learning_experience.fle_activity_type = 'INTERIM_CHECKPOINT' " +
      "THEN dim_interim_checkpoint.ic_dw_id " +
      "ELSE dlo.lo_dw_id END AS fle_lo_dw_id",
    "student1.user_dw_id",
    "dim_subject.subject_dw_id",
    "dim_grade.grade_dw_id",
    "dc.class_dw_id",
    "dim_curriculum_subject.curr_subject_dw_id",
    "dim_curriculum_grade.curr_grade_dw_id",
    "dim_term.term_dw_id",
    "dim_tenant.tenant_dw_id",
    "dim_school.school_dw_id",
    "dim_section.section_dw_id",
    "dim_academic_year.academic_year_dw_id"
  )

  def getSelectQuery(cols: List[String], connection: WarehouseConnection): String = {

    s""" select ${cols.mkString(",\n\t")}
       |from ${connection.schema}_stage.staging_learning_experience
       | left join (select lo_id, lo_dw_id from (
       |         select lo_id, lo_dw_id, row_number() over (partition by lo_id order by lo_created_time desc) as rank
       |         from ${connection.schema}.dim_learning_objective
       |          ) t where t.rank = 1
       |       ) AS dlo ON dlo.lo_id = staging_learning_experience.lo_uuid
       | left join ${connection.schema}.dim_interim_checkpoint
       |    on dim_interim_checkpoint.ic_id = staging_learning_experience.lo_uuid
       |inner join ${connection.schema}_stage.rel_user student1
       |    on student1.user_id = staging_learning_experience.student_uuid
       |left join ${connection.schema}.dim_subject
       |    on dim_subject.subject_id = staging_learning_experience.subject_uuid
       |inner join ${connection.schema}.dim_grade
       |    on dim_grade.grade_id = staging_learning_experience.grade_uuid
       |left join ${connection.schema}.dim_curriculum_subject
       |    on dim_curriculum_subject.curr_subject_id = staging_learning_experience.curr_subject_uuid and
       |                                    dim_curriculum_subject.curr_subject_status = 1
       |left join ${connection.schema}.dim_curriculum_grade
       |    on dim_curriculum_grade.curr_grade_id = staging_learning_experience.curr_grade_uuid
       |inner join ${connection.schema}.dim_tenant
       |    on dim_tenant.tenant_id = staging_learning_experience.tenant_uuid
       |inner join ${connection.schema}.dim_school
       |    on dim_school.school_id = staging_learning_experience.school_uuid
       |left join ${connection.schema}.dim_section
       |    on dim_section.section_id = staging_learning_experience.section_uuid
       |left join ${connection.schema}.dim_academic_year
       |    on dim_academic_year.academic_year_id = staging_learning_experience.academic_year_uuid
       |left join (select class_dw_id, class_id
       |                    from (select
       |                              class_dw_id,
       |                              class_id,
       |                              row_number() over (partition by class_id order by class_created_time desc) as rank
       |                              from ${connection.schema}.dim_class
       |                          ) where rank = 1
       |                    ) as dc
       |                   on dc.class_id = staging_learning_experience.class_uuid
       |
       |left join (select instructional_plan_id,
       |                            instructional_plan_item_lo_dw_id,
       |                            instructional_plan_item_ic_dw_id,
       |                            instructional_plan_item_week_dw_id,
       |                            instructional_plan_item_type
       |                     from
       |                        (select instructional_plan_id,
       |                            nvl(instructional_plan_item_lo_dw_id, -1) as instructional_plan_item_lo_dw_id,
       |                            nvl(instructional_plan_item_ic_dw_id, -1) as instructional_plan_item_ic_dw_id,
       |                            instructional_plan_item_week_dw_id,
       |                            instructional_plan_item_type,
       |                            row_number() over (partition by instructional_plan_id,
       |                                                            nvl(instructional_plan_item_lo_dw_id, -1),
       |                                                            nvl(instructional_plan_item_ic_dw_id, -1)
       |                                                order by instructional_plan_created_time desc) as rank
       |                     from ${connection.schema}.dim_instructional_plan
       |                    ) where rank = 1) as ip on
       |                        ip.instructional_plan_id = staging_learning_experience.fle_material_id and
       |                        staging_learning_experience.fle_material_type = 'INSTRUCTIONAL_PLAN' and
       |                        (dlo.lo_dw_id is not null and
       |                         ip.instructional_plan_item_lo_dw_id = dlo.lo_dw_id and
       |                         nvl(ip.instructional_plan_item_type, '') != 'TEST' or
       |                         dim_interim_checkpoint.ic_dw_id is not null and
       |                         staging_learning_experience.fle_activity_type = 'INTERIM_CHECKPOINT' and
       |                         ip.instructional_plan_item_ic_dw_id = dim_interim_checkpoint.ic_dw_id)
       |
       |left join ${connection.schema}.dim_week
       |      on ip.instructional_plan_item_week_dw_id = dim_week.week_dw_id
       |left join ${connection.schema}.dim_term
       |      on dim_term.term_id = dim_week.week_term_id
       |where
       | ((subject_uuid isnull and dim_subject.subject_dw_id isnull) or (subject_uuid notnull and dim_subject.subject_dw_id notnull))  and
       | ((section_uuid isnull and dim_section.section_dw_id isnull) or (section_uuid notnull and dim_section.section_dw_id notnull))  and
       | ((class_uuid isnull and dc.class_dw_id isnull) or (class_uuid notnull and dc.class_dw_id notnull)) and
       | (dlo.lo_id is not null or dim_interim_checkpoint.ic_id is not null) and
       | (dim_term.term_dw_id is not null and staging_learning_experience.fle_material_type = 'INSTRUCTIONAL_PLAN' or dim_term.term_dw_id is null and staging_learning_experience.fle_material_type IN ('PATHWAY', 'CORE')) AND
       | ((staging_learning_experience.academic_year_uuid IS NULL AND dim_academic_year.academic_year_dw_id IS NULL) OR (staging_learning_experience.academic_year_uuid IS NOT NULL AND dim_academic_year.academic_year_dw_id IS NOT NULL))
       |
       |order by ${pkCol.mkString(",")}
       |limit $QUERY_LIMIT
       |""".stripMargin

  }

}
