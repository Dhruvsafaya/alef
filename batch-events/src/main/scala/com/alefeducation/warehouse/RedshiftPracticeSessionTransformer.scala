package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.{CommonSessionTransformer, SqlStringData}
import scalikejdbc.DBSession

object RedshiftPracticeSessionTransformer extends CommonSessionTransformer {

  override val factEntity: String = "practice"

  override def tableNotation: Map[String, String] = Map()

  override def columnNotation: Map[String, String] = Map()

  override def pkNotation: Map[String, String] = Map()

  override def prepareSqlStrings(schema: String, factCols: String): SqlStringData = {

    val dimJoinsForItemAndContent =
      s"""
         join $schema.dim_learning_objective dlo1 on st.practice_session_item_lo_uuid = dlo1.lo_id
       """.stripMargin

    val commonPracticeSelfJoins =
      s"""
        ${schema}_stage.staging_practice_session st
                     join ${schema}_stage.staging_practice_session en on st.practice_session_id = en.practice_session_id
                       and st.practice_session_event_type = en.practice_session_event_type
                       and st.practice_session_is_start = true and
                     en.practice_session_is_start = false
      """.stripMargin

    val commonDimJoins =
      s"""   join $schema.dim_learning_objective dlo on st.lo_uuid = dlo.lo_id
             join ${schema}_stage.rel_user ru on st.student_uuid = ru.user_id
             left join $schema.dim_subject dsu on st.subject_uuid = dsu.subject_id
             join $schema.dim_grade dg on st.grade_uuid = dg.grade_id
             join $schema.dim_tenant dt on st.tenant_uuid = dt.tenant_id
             join $schema.dim_school ds on st.school_uuid = ds.school_id
             left join $schema.dim_section dsec on st.section_uuid = dsec.section_id
             left join $schema.dim_academic_year day on st.academic_year_uuid = day.academic_year_id
             left join $schema.dim_class dcls on st.class_uuid = dcls.class_id and dcls.class_status = 1
        """.stripMargin

    val commonSelect =
      """select
             st.practice_session_staging_id               as start_id,
             en.practice_session_staging_id               as end_id,
             st.practice_session_created_time             as practice_session_start_time,
             en.practice_session_created_time             as practice_session_end_time,
             getdate()                                    as practice_session_dw_created_time,
             en.practice_session_date_dw_id               as practice_session_date_dw_id,
             st.practice_session_id                       as practice_session_id,
             dlo.lo_dw_id                                 as practice_session_lo_dw_id,
             ru.user_dw_id                                as practice_session_student_dw_id,
             dsu.subject_dw_id                            as practice_session_subject_dw_id,
             dg.grade_dw_id                               as practice_session_grade_dw_id,
             dt.tenant_dw_id                              as practice_session_tenant_dw_id,
             ds.school_dw_id                              as practice_session_school_dw_id,
             dsec.section_dw_id                           as practice_session_section_dw_id,
             st.practice_session_item_content_uuid        as practice_session_item_content_uuid,
             st.practice_session_item_content_title       as practice_session_item_content_title,
             st.practice_session_item_content_lesson_type as practice_session_item_content_lesson_type,
             st.practice_session_item_content_location    as practice_session_item_content_location,
             st.practice_session_sa_score                 as practice_session_sa_score,
             en.practice_session_score                    as practice_session_score,
             st.practice_session_event_type               as practice_session_event_type,
             DATEDIFF(second, st.practice_session_created_time,
                              en.practice_session_created_time)   as practice_session_time_spent,
             en.practice_session_is_start                as practice_session_is_start,
             en.practice_session_outside_of_school       as practice_session_outside_of_school,
             en.practice_session_stars                   as practice_session_stars,
             day.academic_year_dw_id                     as practice_session_academic_year_dw_id,
             en.practice_session_instructional_plan_id   as practice_session_instructional_plan_id,
             en.practice_session_learning_path_id        as practice_session_learning_path_id,
             dcls.class_dw_id                            as practice_session_class_dw_id,
             en.practice_session_material_id             as practice_session_material_id,
             en.practice_session_material_type           as practice_session_material_type"""

    val whereConditionsForOptionalCols =
      """|(
         | ((st.subject_uuid isnull and dsu.subject_dw_id isnull) or (st.subject_uuid notnull and dsu.subject_dw_id notnull)) and
         | ((st.section_uuid isnull and dsec.section_dw_id isnull) or (st.section_uuid notnull and dsec.section_dw_id notnull)) and
         | ((st.class_uuid isnull and dcls.class_dw_id isnull) or (st.class_uuid notnull and dcls.class_dw_id notnull)) and
         | ((st.academic_year_uuid isnull and day.academic_year_dw_id isnull) or (st.academic_year_uuid notnull and day.academic_year_dw_id notnull))
         |)""".stripMargin

    val unionSelect =
      s"""$commonSelect,
             null                                          as practice_session_item_lo_dw_id,
             en.practice_session_item_step_id            as practice_session_item_step_id
      from  $commonPracticeSelfJoins
            $commonDimJoins
      where st.practice_session_event_type = 1 and $whereConditionsForOptionalCols

      union all
               $commonSelect,
               dlo1.lo_dw_id                               as practice_session_item_lo_dw_id,
               en.practice_session_item_step_id            as practice_session_item_step_id
      from   $commonPracticeSelfJoins and st.practice_session_item_lo_uuid = en.practice_session_item_lo_uuid
             $commonDimJoins
             $dimJoinsForItemAndContent
      where st.practice_session_event_type = 2 and $whereConditionsForOptionalCols

      union all
               $commonSelect,
               dlo1.lo_dw_id                               as practice_session_item_lo_dw_id,
               en.practice_session_item_step_id            as practice_session_item_step_id
      from $commonPracticeSelfJoins and st.practice_session_item_lo_uuid = en.practice_session_item_lo_uuid
                                and st.practice_session_item_step_id = en.practice_session_item_step_id
             $commonDimJoins
             $dimJoinsForItemAndContent
      where st.practice_session_event_type = 3 and $whereConditionsForOptionalCols""".stripMargin

    val insertColsFact = s"""insert into $schema.fact_practice_session($factCols)"""

    val selectPracticeFactCols =
      """select
              a.practice_session_start_time,
              a.practice_session_end_time,
              a.practice_session_dw_created_time,
              a.practice_session_date_dw_id,
              a.practice_session_id,
              a.practice_session_lo_dw_id,
              a.practice_session_student_dw_id,
              a.practice_session_subject_dw_id,
              a.practice_session_grade_dw_id,
              a.practice_session_tenant_dw_id,
              a.practice_session_school_dw_id,
              a.practice_session_section_dw_id,
              a.practice_session_sa_score,
              a.practice_session_item_lo_dw_id,
              a.practice_session_item_content_uuid,
              a.practice_session_item_content_title,
              a.practice_session_item_content_lesson_type,
              a.practice_session_item_content_location,
              a.practice_session_time_spent,
              a.practice_session_score,
              a.practice_session_event_type,
              a.practice_session_is_start,
              a.practice_session_outside_of_school,
              a.practice_session_stars,
              a.practice_session_academic_year_dw_id,
              a.practice_session_instructional_plan_id,
              a.practice_session_learning_path_id,
              a.practice_session_class_dw_id,
              a.practice_session_item_step_id,
              a.practice_session_material_id,
              a.practice_session_material_type"""

    val insertSql = (idsClause: String) =>
      s"""$insertColsFact
         |$selectPracticeFactCols
         |from ($unionSelect) as a where (a.start_id in ($idsClause) or a.end_id in ($idsClause)) order by a.start_id limit $QUERY_LIMIT;
      """.stripMargin

    val idSelectForDelete =
      s"""select a.start_id, a.end_id from ($unionSelect) as a order by a.start_id limit $QUERY_LIMIT;""".stripMargin

    // IN_PROGRESS events to be put in fact
    val commonSelectForStart =
      """select
        |       st.practice_session_staging_id               as start_id,
        |       st.practice_session_created_time             as practice_session_start_time,
        |       cast(null as timestamp)                      as practice_session_end_time,
        |       getdate()                                    as practice_session_dw_created_time,
        |       st.practice_session_date_dw_id               as practice_session_date_dw_id,
        |       st.practice_session_id                       as practice_session_id,
        |       dlo.lo_dw_id                                 as practice_session_lo_dw_id,
        |       ru.user_dw_id                                as practice_session_student_dw_id,
        |       dsu.subject_dw_id                            as practice_session_subject_dw_id,
        |       dg.grade_dw_id                               as practice_session_grade_dw_id,
        |       dt.tenant_dw_id                              as practice_session_tenant_dw_id,
        |       ds.school_dw_id                              as practice_session_school_dw_id,
        |       dsec.section_dw_id                           as practice_session_section_dw_id,
        |       st.practice_session_sa_score                 as practice_session_sa_score,
        |       st.practice_session_item_content_uuid        as practice_session_item_content_uuid,
        |       st.practice_session_item_content_title       as practice_session_item_content_title,
        |       st.practice_session_item_content_lesson_type as practice_session_item_content_lesson_type,
        |       st.practice_session_item_content_location    as practice_session_item_content_location,
        |       st.practice_session_event_type               as practice_session_event_type,
        |       cast(null as int)                            as practice_session_time_spent,
        |       cast(null as bigint)                         as practice_session_score,
        |       st.practice_session_is_start                 as practice_session_is_start,
        |       st.practice_session_outside_of_school        as practice_session_outside_of_school,
        |       st.practice_session_stars                    as practice_session_stars,
        |       day.academic_year_dw_id                      as practice_session_academic_year_dw_id,
        |       st.practice_session_instructional_plan_id    as practice_session_instructional_plan_id,
        |       st.practice_session_learning_path_id         as practice_session_learning_path_id,
        |       dcls.class_dw_id                             as practice_session_class_dw_id,
        |       st.practice_session_material_id              as practice_session_material_id,
        |       st.practice_session_material_type            as practice_session_material_type""".stripMargin

    val commonWhereClause =
      s"""
        where st.practice_session_is_start = true
        and st.practice_session_is_start_event_processed = false
        and $whereConditionsForOptionalCols
      """.stripMargin

    val startUnionSelect =
      s"""$commonSelectForStart,
         | null                                        as practice_session_item_lo_dw_id,
         | st.practice_session_item_step_id            as practice_session_item_step_id
         | from ${schema}_stage.staging_practice_session st
         | $commonDimJoins
         | $commonWhereClause and st.practice_session_event_type = 1
         |
         | union all
         |
         | $commonSelectForStart,
         | dlo1.lo_dw_id                               as practice_session_item_lo_dw_id,
         | st.practice_session_item_step_id            as practice_session_item_step_id
         | from ${schema}_stage.staging_practice_session st
         | $commonDimJoins
         | $dimJoinsForItemAndContent
         | $commonWhereClause and st.practice_session_event_type IN (2, 3)""".stripMargin

    val startEventsInsert = (idsClause: String) =>
      s"""$insertColsFact $selectPracticeFactCols from ($startUnionSelect) as a where a.start_id in ($idsClause) order by a.start_id limit $QUERY_LIMIT;""".stripMargin

    val idSelectForUpdate =
      s"""select a.start_id from ($startUnionSelect) as a order by a.start_id limit $QUERY_LIMIT;""".stripMargin

    log.info(s"start events insert query: ${startEventsInsert("ids")}")
    log.info(s"ids select for update query: $idSelectForUpdate")
    log.info(s"insert query: ${insertSql("ids")}")
    log.info(s"ids select for delete query: $idSelectForDelete")

    SqlStringData(insertSql, startEventsInsert, idSelectForDelete, idSelectForUpdate)
  }

  /**
   * if we change order from original we first save all start events and then process all end events which has start events.
   * @param schema
   * @param factData
   */
  override def processEvents(schema: String, factData: SqlStringData): Unit = {
    processStartEvents(schema, factData.startEventsInsert, factData.idSelectForUpdate)
    processCompletedEvents(schema, factData.insertSql, factData.idSelectForDelete)
  }

}