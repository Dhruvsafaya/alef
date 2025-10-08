package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.{CommonSessionTransformer, SqlStringData}

object RedshiftKTGameSessionTransformer extends CommonSessionTransformer {

  override val factEntity: String = "ktg"

  override def tableNotation: Map[String, String] = Map()

  override def columnNotation: Map[String, String] = Map()

  override def pkNotation: Map[String, String] = Map()

  override def prepareSqlStrings(schema: String, factCols: String): SqlStringData = {
    val commonKTSelfJoins =
      s"""
        ${schema}_stage.staging_ktg_session st
                     join ${schema}_stage.staging_ktg_session en on st.ktg_session_id = en.ktg_session_id
                       and st.ktg_session_event_type = en.ktg_session_event_type
                       and st.ktg_session_is_start = true and
                     en.ktg_session_is_start = false
      """.stripMargin

    val commonDimJoins =
      s"""   join ${schema}_stage.rel_user ru on st.student_uuid = ru.user_id
             left join $schema.dim_subject dsu on st.subject_uuid = dsu.subject_id
             join $schema.dim_grade dg on st.grade_uuid = dg.grade_id
             join $schema.dim_tenant dt on st.tenant_uuid = dt.tenant_id
             join $schema.dim_school ds on st.school_uuid = ds.school_id
             left join $schema.dim_section dc on st.section_uuid = dc.section_id
             left join $schema.dim_academic_year day on st.academic_year_uuid = day.academic_year_id
             left join $schema.dim_class dcls on st.class_uuid = dcls.class_id and dcls.class_status = 1
        """.stripMargin

    val commonSelect =
      """select
             st.ktg_session_staging_id               as start_id,
             en.ktg_session_staging_id               as end_id,
             st.ktg_session_id                       as ktg_session_id,
             st.ktg_session_created_time             as ktg_session_start_time,
             en.ktg_session_created_time             as ktg_session_end_time,
             getdate()                               as ktg_session_dw_created_time,
             en.ktg_session_date_dw_id               as ktg_session_date_dw_id,
             en.ktg_session_question_id              as ktg_session_question_id,
             en.ktg_session_kt_id                    as ktg_session_kt_id,
             dt.tenant_dw_id                         as ktg_session_tenant_dw_id,
             ru.user_dw_id                           as ktg_session_student_dw_id,
             dsu.subject_dw_id                       as ktg_session_subject_dw_id,
             ds.school_dw_id                         as ktg_session_school_dw_id,
             dg.grade_dw_id                          as ktg_session_grade_dw_id,
             dc.section_dw_id                        as ktg_session_section_dw_id,
             day.academic_year_dw_id                 as ktg_session_academic_year_dw_id,
             cast(en.ktg_session_outside_of_school as boolean)        as ktg_session_outside_of_school,
             en.ktg_session_trimester_id             as ktg_session_trimester_id,
             en.ktg_session_trimester_order          as ktg_session_trimester_order,
             en.ktg_session_type                     as ktg_session_type,
             en.ktg_session_question_time_allotted   as ktg_session_question_time_allotted,
             DATEDIFF(second, st.ktg_session_created_time,
                              en.ktg_session_created_time)   as ktg_session_time_spent,
             en.ktg_session_answer                   as ktg_session_answer,
             en.ktg_session_num_attempts             as ktg_session_num_attempts,
             en.ktg_session_score                    as ktg_session_score,
             en.ktg_session_max_score                as ktg_session_max_score,
             en.ktg_session_stars                    as ktg_session_stars,
             en.ktg_session_is_attended                    as ktg_session_is_attended,
             en.ktg_session_event_type               as ktg_session_event_type,
             en.ktg_session_is_start                 as ktg_session_is_start,
             en.ktg_session_instructional_plan_id    as ktg_session_instructional_plan_id,
             en.ktg_session_learning_path_id         as ktg_session_learning_path_id,
             dcls.class_dw_id                        as ktg_session_session_class_dw_id,
             en.ktg_session_material_id              as ktg_session_material_id,
             en.ktg_session_material_type            as ktg_session_material_type"""

    val whereConditionsForOptionalCols =
      """|(
         |  ((st.subject_uuid isnull and dsu.subject_dw_id isnull) or (st.subject_uuid notnull and dsu.subject_dw_id notnull))  and
         |  ((st.section_uuid isnull and dc.section_dw_id isnull) or (st.section_uuid notnull and dc.section_dw_id notnull))  and
         |  ((st.class_uuid isnull and dcls.class_dw_id isnull) or (st.class_uuid notnull and dcls.class_dw_id notnull))  and
         |  ((st.academic_year_uuid isnull and day.academic_year_dw_id isnull) or (st.academic_year_uuid notnull and day.academic_year_dw_id notnull))
         |)""".stripMargin

    val unionSelect =
      s"""$commonSelect,
             cast(null as bigint)                    as ktg_session_lo_dw_id
      from  $commonKTSelfJoins
            $commonDimJoins
      where st.ktg_session_event_type = 1
        and $whereConditionsForOptionalCols

      union all
               $commonSelect,
               dlo.lo_dw_id                               as ktg_session_lo_dw_id
      from   $commonKTSelfJoins and st.ktg_session_question_id = en.ktg_session_question_id
             $commonDimJoins join $schema.dim_learning_objective dlo on en.lo_uuid = dlo.lo_id
      where st.ktg_session_event_type = 2
         and $whereConditionsForOptionalCols""".stripMargin

    val insertColsFact =
      s"""insert into $schema.fact_ktg_session($factCols)"""

    val selectKTFactCols =
      """select
              a.ktg_session_id,
              a.ktg_session_start_time,
              a.ktg_session_end_time,
              a.ktg_session_dw_created_time,
              a.ktg_session_date_dw_id,
              a.ktg_session_question_id,
              a.ktg_session_kt_id,
              a.ktg_session_tenant_dw_id,
              a.ktg_session_student_dw_id,
              a.ktg_session_subject_dw_id,
              a.ktg_session_school_dw_id,
              a.ktg_session_grade_dw_id,
              a.ktg_session_section_dw_id,
              a.ktg_session_lo_dw_id,
              a.ktg_session_academic_year_dw_id,
              a.ktg_session_outside_of_school,
              a.ktg_session_trimester_id,
              a.ktg_session_trimester_order,
              a.ktg_session_type,
              a.ktg_session_question_time_allotted,
              a.ktg_session_time_spent,
              a.ktg_session_answer,
              a.ktg_session_num_attempts,
              a.ktg_session_score,
              a.ktg_session_max_score,
              a.ktg_session_stars,
              a.ktg_session_is_attended,
              a.ktg_session_event_type,
              a.ktg_session_is_start,
              a.ktg_session_instructional_plan_id,
              a.ktg_session_learning_path_id,
              a.ktg_session_session_class_dw_id,
              a.ktg_session_material_id,
              a.ktg_session_material_type"""

    val insertSql = (idClause: String) =>
      s"""$insertColsFact
         |$selectKTFactCols
         |from ($unionSelect) as a where (a.start_id in ($idClause) or a.end_id in ($idClause)) order by a.start_id limit $QUERY_LIMIT;
      """.stripMargin

    /*
    Remove started events from staging if they are stuck there for a long period due to missing finished events.
    They will be saved into the main table anyway.
    More details: https://alefeducation.atlassian.net/browse/ALEF-32123
    */
    val deleteDanglingSql =
      s"""
         |DELETE FROM ${schema}_stage.staging_ktg_session
         |USING ${schema}.dim_academic_year ay
         |WHERE
         |    staging_ktg_session.academic_year_uuid = ay.academic_year_id
         |    AND ay.academic_year_status = 1
         |    AND ay.academic_year_end_date < current_date
         |    AND staging_ktg_session.ktg_session_is_start = true
         |    AND staging_ktg_session.ktg_session_is_start_event_processed = true;
         |""".stripMargin

    val idSelectForDelete =
      s"""select a.start_id, a.end_id from ($unionSelect) as a order by a.start_id limit $QUERY_LIMIT;""".stripMargin

    // IN_PROGRESS events to be put in fact
    val commonSelectForStart =
      """select
             st.ktg_session_staging_id               as start_id,
             st.ktg_session_id                       as ktg_session_id,
             st.ktg_session_created_time             as ktg_session_start_time,
             cast(null as timestamp)                 as ktg_session_end_time,
             getdate()                               as ktg_session_dw_created_time,
             st.ktg_session_date_dw_id               as ktg_session_date_dw_id,
             st.ktg_session_question_id              as ktg_session_question_id,
             st.ktg_session_kt_id                    as ktg_session_kt_id,
             dt.tenant_dw_id                         as ktg_session_tenant_dw_id,
             ru.user_dw_id                           as ktg_session_student_dw_id,
             dsu.subject_dw_id                       as ktg_session_subject_dw_id,
             ds.school_dw_id                         as ktg_session_school_dw_id,
             dg.grade_dw_id                          as ktg_session_grade_dw_id,
             dc.section_dw_id                        as ktg_session_section_dw_id,
             day.academic_year_dw_id                 as ktg_session_academic_year_dw_id,
             cast(st.ktg_session_outside_of_school as boolean)    as ktg_session_outside_of_school,
             st.ktg_session_trimester_id             as ktg_session_trimester_id,
             st.ktg_session_trimester_order          as ktg_session_trimester_order,
             st.ktg_session_type                     as ktg_session_type,
             st.ktg_session_question_time_allotted   as ktg_session_question_time_allotted,
             cast(null as int)                       as ktg_session_time_spent,
             st.ktg_session_answer                   as ktg_session_answer,
             st.ktg_session_num_attempts             as ktg_session_num_attempts,
             st.ktg_session_score                    as ktg_session_score,
             st.ktg_session_max_score                as ktg_session_max_score,
             st.ktg_session_stars                    as ktg_session_stars,
             st.ktg_session_is_attended              as ktg_session_is_attended,
             st.ktg_session_event_type               as ktg_session_event_type,
             st.ktg_session_is_start                 as ktg_session_is_start,
             st.ktg_session_instructional_plan_id    as ktg_session_instructional_plan_id,
             st.ktg_session_learning_path_id         as ktg_session_learning_path_id,
             dcls.class_dw_id                        as ktg_session_session_class_dw_id,
             st.ktg_session_material_id              as ktg_session_material_id,
             st.ktg_session_material_type            as ktg_session_material_type""".stripMargin

    val commonWhereClause =
      """
        where st.ktg_session_is_start = true
        and st.ktg_session_is_start_event_processed = false
      """.stripMargin

    val startUnionSelect =
      s"""$commonSelectForStart,
         | cast(null as bigint)  as ktg_session_lo_dw_id
         | from ${schema}_stage.staging_ktg_session st
         | $commonDimJoins
         | $commonWhereClause and st.ktg_session_event_type = 1
         | and $whereConditionsForOptionalCols
         |
         | union all
         |
         | $commonSelectForStart,
         | dlo.lo_dw_id          as ktg_session_lo_dw_id
         | from ${schema}_stage.staging_ktg_session st
         | $commonDimJoins join $schema.dim_learning_objective dlo on st.lo_uuid = dlo.lo_id
         | $commonWhereClause and st.ktg_session_event_type = 2
         | and $whereConditionsForOptionalCols""".stripMargin

    val startEventsInsert = (idClause: String) =>
      s"""$insertColsFact $selectKTFactCols
         from ($startUnionSelect) as a where a.start_id in ($idClause) order by a.start_id limit $QUERY_LIMIT;
      """.stripMargin

    val idSelectForUpdate =
      s"""
         select a.start_id from ($startUnionSelect) as a order by a.start_id limit $QUERY_LIMIT;
      """.stripMargin

    log.info(s"insert query: ${insertSql("ids")}")
    log.info(s"start events insert query: ${startEventsInsert("ids")}")
    log.info(s"ids select for delete query: $idSelectForDelete")
    log.info(s"ids select for update query: $idSelectForUpdate")

    SqlStringData(insertSql, startEventsInsert, idSelectForDelete, idSelectForUpdate, deleteDanglingSql)
  }

}
