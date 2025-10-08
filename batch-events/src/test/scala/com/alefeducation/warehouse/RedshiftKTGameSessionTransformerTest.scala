package com.alefeducation.warehouse

import com.alefeducation.util.StringUtilities.replaceSpecChars
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class RedshiftKTGameSessionTransformerTest extends AnyFunSuite with Matchers {

  val transformer: RedshiftKTGameSessionTransformer.type = RedshiftKTGameSessionTransformer

  val factCols: String =
    """ktg_session_id,
      |ktg_session_start_time,
      |ktg_session_end_time,
      |ktg_session_dw_created_time,
      |ktg_session_date_dw_id,
      |ktg_session_question_id,
      |ktg_session_kt_id,
      |ktg_session_tenant_dw_id,
      |ktg_session_student_dw_id,
      |ktg_session_subject_dw_id,
      |ktg_session_school_dw_id,
      |ktg_session_grade_dw_id,
      |ktg_session_section_dw_id,
      |ktg_session_lo_dw_id,
      |ktg_session_academic_year_dw_id,
      |ktg_session_outside_of_school,
      |ktg_session_trimester_id,
      |ktg_session_trimester_order,
      |ktg_session_type,
      |ktg_session_question_time_allotted,
      |ktg_session_time_spent,
      |ktg_session_answer,
      |ktg_session_num_attempts,
      |ktg_session_score,
      |ktg_session_max_score,
      |ktg_session_stars,
      |ktg_session_is_attended,
      |ktg_session_event_type,
      |ktg_session_is_start,
      |ktg_session_instructional_plan_id,
      |ktg_session_learning_path_id,
      |ktg_session_class_dw_id,
      |ktg_session_material_id,
      |ktg_session_material_type""".stripMargin

  test("KT game should prepare queries for transform data from staging table to main") {
    val sqlStringData = transformer.prepareSqlStrings("alefdw", factCols)

    transformer.factEntity shouldBe "ktg"

    replaceSpecChars(sqlStringData.insertSql("1001, 1002, 1003")) shouldBe replaceSpecChars(
      """
        |insert into alefdw.fact_ktg_session(ktg_session_id, ktg_session_start_time, ktg_session_end_time,
        |                                    ktg_session_dw_created_time, ktg_session_date_dw_id, ktg_session_question_id,
        |                                    ktg_session_kt_id, ktg_session_tenant_dw_id, ktg_session_student_dw_id,
        |                                    ktg_session_subject_dw_id, ktg_session_school_dw_id, ktg_session_grade_dw_id,
        |                                    ktg_session_section_dw_id, ktg_session_lo_dw_id, ktg_session_academic_year_dw_id,
        |                                    ktg_session_outside_of_school, ktg_session_trimester_id,
        |                                    ktg_session_trimester_order, ktg_session_type, ktg_session_question_time_allotted,
        |                                    ktg_session_time_spent, ktg_session_answer, ktg_session_num_attempts,
        |                                    ktg_session_score, ktg_session_max_score, ktg_session_stars,
        |                                    ktg_session_is_attended, ktg_session_event_type, ktg_session_is_start,
        |                                    ktg_session_instructional_plan_id, ktg_session_learning_path_id,
        |                                    ktg_session_class_dw_id, ktg_session_material_id, ktg_session_material_type)
        |select a.ktg_session_id,
        |       a.ktg_session_start_time,
        |       a.ktg_session_end_time,
        |       a.ktg_session_dw_created_time,
        |       a.ktg_session_date_dw_id,
        |       a.ktg_session_question_id,
        |       a.ktg_session_kt_id,
        |       a.ktg_session_tenant_dw_id,
        |       a.ktg_session_student_dw_id,
        |       a.ktg_session_subject_dw_id,
        |       a.ktg_session_school_dw_id,
        |       a.ktg_session_grade_dw_id,
        |       a.ktg_session_section_dw_id,
        |       a.ktg_session_lo_dw_id,
        |       a.ktg_session_academic_year_dw_id,
        |       a.ktg_session_outside_of_school,
        |       a.ktg_session_trimester_id,
        |       a.ktg_session_trimester_order,
        |       a.ktg_session_type,
        |       a.ktg_session_question_time_allotted,
        |       a.ktg_session_time_spent,
        |       a.ktg_session_answer,
        |       a.ktg_session_num_attempts,
        |       a.ktg_session_score,
        |       a.ktg_session_max_score,
        |       a.ktg_session_stars,
        |       a.ktg_session_is_attended,
        |       a.ktg_session_event_type,
        |       a.ktg_session_is_start,
        |       a.ktg_session_instructional_plan_id,
        |       a.ktg_session_learning_path_id,
        |       a.ktg_session_session_class_dw_id,
        |       a.ktg_session_material_id,
        |       a.ktg_session_material_type
        |from (select st.ktg_session_staging_id                                                  as start_id,
        |             en.ktg_session_staging_id                                                  as end_id,
        |             st.ktg_session_id                                                          as ktg_session_id,
        |             st.ktg_session_created_time                                                as ktg_session_start_time,
        |             en.ktg_session_created_time                                                as ktg_session_end_time,
        |             getdate()                                                                  as ktg_session_dw_created_time,
        |             en.ktg_session_date_dw_id                                                  as ktg_session_date_dw_id,
        |             en.ktg_session_question_id                                                 as ktg_session_question_id,
        |             en.ktg_session_kt_id                                                       as ktg_session_kt_id,
        |             dt.tenant_dw_id                                                            as ktg_session_tenant_dw_id,
        |             ru.user_dw_id                                                              as ktg_session_student_dw_id,
        |             dsu.subject_dw_id                                                          as ktg_session_subject_dw_id,
        |             ds.school_dw_id                                                            as ktg_session_school_dw_id,
        |             dg.grade_dw_id                                                             as ktg_session_grade_dw_id,
        |             dc.section_dw_id                                                           as ktg_session_section_dw_id,
        |             day.academic_year_dw_id                                                    as ktg_session_academic_year_dw_id,
        |             cast(en.ktg_session_outside_of_school as boolean)                          as ktg_session_outside_of_school,
        |             en.ktg_session_trimester_id                                                as ktg_session_trimester_id,
        |             en.ktg_session_trimester_order                                             as ktg_session_trimester_order,
        |             en.ktg_session_type                                                        as ktg_session_type,
        |             en.ktg_session_question_time_allotted                                      as ktg_session_question_time_allotted,
        |             DATEDIFF(second, st.ktg_session_created_time, en.ktg_session_created_time) as ktg_session_time_spent,
        |             en.ktg_session_answer                                                      as ktg_session_answer,
        |             en.ktg_session_num_attempts                                                as ktg_session_num_attempts,
        |             en.ktg_session_score                                                       as ktg_session_score,
        |             en.ktg_session_max_score                                                   as ktg_session_max_score,
        |             en.ktg_session_stars                                                       as ktg_session_stars,
        |             en.ktg_session_is_attended                                                 as ktg_session_is_attended,
        |             en.ktg_session_event_type                                                  as ktg_session_event_type,
        |             en.ktg_session_is_start                                                    as ktg_session_is_start,
        |             en.ktg_session_instructional_plan_id                                       as ktg_session_instructional_plan_id,
        |             en.ktg_session_learning_path_id                                            as ktg_session_learning_path_id,
        |             dcls.class_dw_id                                                           as ktg_session_session_class_dw_id,
        |             en.ktg_session_material_id                                                 as ktg_session_material_id,
        |             en.ktg_session_material_type                                               as ktg_session_material_type,
        |             cast(null as bigint)                                                       as ktg_session_lo_dw_id
        |      from alefdw_stage.staging_ktg_session st
        |               join alefdw_stage.staging_ktg_session en on st.ktg_session_id = en.ktg_session_id and
        |                                                           st.ktg_session_event_type = en.ktg_session_event_type and
        |                                                           st.ktg_session_is_start = true and
        |                                                           en.ktg_session_is_start = false
        |               join alefdw_stage.rel_user ru on st.student_uuid = ru.user_id
        |               left join alefdw.dim_subject dsu on st.subject_uuid = dsu.subject_id
        |               join alefdw.dim_grade dg on st.grade_uuid = dg.grade_id
        |               join alefdw.dim_tenant dt on st.tenant_uuid = dt.tenant_id
        |               join alefdw.dim_school ds on st.school_uuid = ds.school_id
        |               left join alefdw.dim_section dc on st.section_uuid = dc.section_id
        |               left join alefdw.dim_academic_year day on st.academic_year_uuid = day.academic_year_id
        |               left join alefdw.dim_class dcls on st.class_uuid = dcls.class_id and dcls.class_status = 1
        |      where st.ktg_session_event_type = 1
        |        and (
        |         ((st.subject_uuid isnull and dsu.subject_dw_id isnull) or (st.subject_uuid notnull and dsu.subject_dw_id notnull)) and
        |         ((st.section_uuid isnull and dc.section_dw_id isnull) or (st.section_uuid notnull and dc.section_dw_id notnull)) and
        |         ((st.class_uuid isnull and dcls.class_dw_id isnull) or (st.class_uuid notnull and dcls.class_dw_id notnull)) and
        |         ((st.academic_year_uuid isnull and day.academic_year_dw_id isnull) or (st.academic_year_uuid notnull and day.academic_year_dw_id notnull))
        |        )
        |      union all
        |      select st.ktg_session_staging_id                                                  as start_id,
        |             en.ktg_session_staging_id                                                  as end_id,
        |             st.ktg_session_id                                                          as ktg_session_id,
        |             st.ktg_session_created_time                                                as ktg_session_start_time,
        |             en.ktg_session_created_time                                                as ktg_session_end_time,
        |             getdate()                                                                  as ktg_session_dw_created_time,
        |             en.ktg_session_date_dw_id                                                  as ktg_session_date_dw_id,
        |             en.ktg_session_question_id                                                 as ktg_session_question_id,
        |             en.ktg_session_kt_id                                                       as ktg_session_kt_id,
        |             dt.tenant_dw_id                                                            as ktg_session_tenant_dw_id,
        |             ru.user_dw_id                                                              as ktg_session_student_dw_id,
        |             dsu.subject_dw_id                                                          as ktg_session_subject_dw_id,
        |             ds.school_dw_id                                                            as ktg_session_school_dw_id,
        |             dg.grade_dw_id                                                             as ktg_session_grade_dw_id,
        |             dc.section_dw_id                                                           as ktg_session_section_dw_id,
        |             day.academic_year_dw_id                                                    as ktg_session_academic_year_dw_id,
        |             cast(en.ktg_session_outside_of_school as boolean)                          as ktg_session_outside_of_school,
        |             en.ktg_session_trimester_id                                                as ktg_session_trimester_id,
        |             en.ktg_session_trimester_order                                             as ktg_session_trimester_order,
        |             en.ktg_session_type                                                        as ktg_session_type,
        |             en.ktg_session_question_time_allotted                                      as ktg_session_question_time_allotted,
        |             DATEDIFF(second, st.ktg_session_created_time, en.ktg_session_created_time) as ktg_session_time_spent,
        |             en.ktg_session_answer                                                      as ktg_session_answer,
        |             en.ktg_session_num_attempts                                                as ktg_session_num_attempts,
        |             en.ktg_session_score                                                       as ktg_session_score,
        |             en.ktg_session_max_score                                                   as ktg_session_max_score,
        |             en.ktg_session_stars                                                       as ktg_session_stars,
        |             en.ktg_session_is_attended                                                 as ktg_session_is_attended,
        |             en.ktg_session_event_type                                                  as ktg_session_event_type,
        |             en.ktg_session_is_start                                                    as ktg_session_is_start,
        |             en.ktg_session_instructional_plan_id                                       as ktg_session_instructional_plan_id,
        |             en.ktg_session_learning_path_id                                            as ktg_session_learning_path_id,
        |             dcls.class_dw_id                                                           as ktg_session_session_class_dw_id,
        |             en.ktg_session_material_id                                                 as ktg_session_material_id,
        |             en.ktg_session_material_type                                               as ktg_session_material_type,
        |             dlo.lo_dw_id                                                               as ktg_session_lo_dw_id
        |      from alefdw_stage.staging_ktg_session st
        |               join alefdw_stage.staging_ktg_session en on st.ktg_session_id = en.ktg_session_id and
        |                                                           st.ktg_session_event_type = en.ktg_session_event_type and
        |                                                           st.ktg_session_is_start = true and
        |                                                           en.ktg_session_is_start = false and
        |                                                           st.ktg_session_question_id = en.ktg_session_question_id
        |               join alefdw_stage.rel_user ru on st.student_uuid = ru.user_id
        |               left join alefdw.dim_subject dsu on st.subject_uuid = dsu.subject_id
        |               join alefdw.dim_grade dg on st.grade_uuid = dg.grade_id
        |               join alefdw.dim_tenant dt on st.tenant_uuid = dt.tenant_id
        |               join alefdw.dim_school ds on st.school_uuid = ds.school_id
        |               left join alefdw.dim_section dc on st.section_uuid = dc.section_id
        |               left join alefdw.dim_academic_year day on st.academic_year_uuid = day.academic_year_id
        |               left join alefdw.dim_class dcls on st.class_uuid = dcls.class_id and dcls.class_status = 1
        |               join alefdw.dim_learning_objective dlo on en.lo_uuid = dlo.lo_id
        |      where st.ktg_session_event_type = 2
        |        and (
        |         ((st.subject_uuid isnull and dsu.subject_dw_id isnull) or (st.subject_uuid notnull and dsu.subject_dw_id notnull)) and
        |         ((st.section_uuid isnull and dc.section_dw_id isnull) or (st.section_uuid notnull and dc.section_dw_id notnull)) and
        |         ((st.class_uuid isnull and dcls.class_dw_id isnull) or (st.class_uuid notnull and dcls.class_dw_id notnull)) and
        |         ((st.academic_year_uuid isnull and day.academic_year_dw_id isnull) or (st.academic_year_uuid notnull and day.academic_year_dw_id notnull))
        |        )) as a
        |where (a.start_id in (1001, 1002, 1003) or a.end_id in (1001, 1002, 1003))
        |order by a.start_id
        |limit 60000;
        |""".stripMargin)


    replaceSpecChars(sqlStringData.startEventsInsert("1001, 1002, 1003")) shouldBe replaceSpecChars(
      """
        |insert into alefdw.fact_ktg_session(ktg_session_id, ktg_session_start_time, ktg_session_end_time,
        |                                    ktg_session_dw_created_time, ktg_session_date_dw_id, ktg_session_question_id,
        |                                    ktg_session_kt_id, ktg_session_tenant_dw_id, ktg_session_student_dw_id,
        |                                    ktg_session_subject_dw_id, ktg_session_school_dw_id, ktg_session_grade_dw_id,
        |                                    ktg_session_section_dw_id, ktg_session_lo_dw_id, ktg_session_academic_year_dw_id,
        |                                    ktg_session_outside_of_school, ktg_session_trimester_id,
        |                                    ktg_session_trimester_order, ktg_session_type, ktg_session_question_time_allotted,
        |                                    ktg_session_time_spent, ktg_session_answer, ktg_session_num_attempts,
        |                                    ktg_session_score, ktg_session_max_score, ktg_session_stars,
        |                                    ktg_session_is_attended, ktg_session_event_type, ktg_session_is_start,
        |                                    ktg_session_instructional_plan_id, ktg_session_learning_path_id,
        |                                    ktg_session_class_dw_id, ktg_session_material_id, ktg_session_material_type)
        |select a.ktg_session_id,
        |       a.ktg_session_start_time,
        |       a.ktg_session_end_time,
        |       a.ktg_session_dw_created_time,
        |       a.ktg_session_date_dw_id,
        |       a.ktg_session_question_id,
        |       a.ktg_session_kt_id,
        |       a.ktg_session_tenant_dw_id,
        |       a.ktg_session_student_dw_id,
        |       a.ktg_session_subject_dw_id,
        |       a.ktg_session_school_dw_id,
        |       a.ktg_session_grade_dw_id,
        |       a.ktg_session_section_dw_id,
        |       a.ktg_session_lo_dw_id,
        |       a.ktg_session_academic_year_dw_id,
        |       a.ktg_session_outside_of_school,
        |       a.ktg_session_trimester_id,
        |       a.ktg_session_trimester_order,
        |       a.ktg_session_type,
        |       a.ktg_session_question_time_allotted,
        |       a.ktg_session_time_spent,
        |       a.ktg_session_answer,
        |       a.ktg_session_num_attempts,
        |       a.ktg_session_score,
        |       a.ktg_session_max_score,
        |       a.ktg_session_stars,
        |       a.ktg_session_is_attended,
        |       a.ktg_session_event_type,
        |       a.ktg_session_is_start,
        |       a.ktg_session_instructional_plan_id,
        |       a.ktg_session_learning_path_id,
        |       a.ktg_session_session_class_dw_id,
        |       a.ktg_session_material_id,
        |       a.ktg_session_material_type
        |from (select st.ktg_session_staging_id                         as start_id,
        |             st.ktg_session_id                                 as ktg_session_id,
        |             st.ktg_session_created_time                       as ktg_session_start_time,
        |             cast(null as timestamp)                           as ktg_session_end_time,
        |             getdate()                                         as ktg_session_dw_created_time,
        |             st.ktg_session_date_dw_id                         as ktg_session_date_dw_id,
        |             st.ktg_session_question_id                        as ktg_session_question_id,
        |             st.ktg_session_kt_id                              as ktg_session_kt_id,
        |             dt.tenant_dw_id                                   as ktg_session_tenant_dw_id,
        |             ru.user_dw_id                                     as ktg_session_student_dw_id,
        |             dsu.subject_dw_id                                 as ktg_session_subject_dw_id,
        |             ds.school_dw_id                                   as ktg_session_school_dw_id,
        |             dg.grade_dw_id                                    as ktg_session_grade_dw_id,
        |             dc.section_dw_id                                  as ktg_session_section_dw_id,
        |             day.academic_year_dw_id                           as ktg_session_academic_year_dw_id,
        |             cast(st.ktg_session_outside_of_school as boolean) as ktg_session_outside_of_school,
        |             st.ktg_session_trimester_id                       as ktg_session_trimester_id,
        |             st.ktg_session_trimester_order                    as ktg_session_trimester_order,
        |             st.ktg_session_type                               as ktg_session_type,
        |             st.ktg_session_question_time_allotted             as ktg_session_question_time_allotted,
        |             cast(null as int)                                 as ktg_session_time_spent,
        |             st.ktg_session_answer                             as ktg_session_answer,
        |             st.ktg_session_num_attempts                       as ktg_session_num_attempts,
        |             st.ktg_session_score                              as ktg_session_score,
        |             st.ktg_session_max_score                          as ktg_session_max_score,
        |             st.ktg_session_stars                              as ktg_session_stars,
        |             st.ktg_session_is_attended                        as ktg_session_is_attended,
        |             st.ktg_session_event_type                         as ktg_session_event_type,
        |             st.ktg_session_is_start                           as ktg_session_is_start,
        |             st.ktg_session_instructional_plan_id              as ktg_session_instructional_plan_id,
        |             st.ktg_session_learning_path_id                   as ktg_session_learning_path_id,
        |             dcls.class_dw_id                                  as ktg_session_session_class_dw_id,
        |             st.ktg_session_material_id                        as ktg_session_material_id,
        |             st.ktg_session_material_type                      as ktg_session_material_type,
        |             cast(null as bigint)                              as ktg_session_lo_dw_id
        |      from alefdw_stage.staging_ktg_session st
        |               join alefdw_stage.rel_user ru on st.student_uuid = ru.user_id
        |               left join alefdw.dim_subject dsu on st.subject_uuid = dsu.subject_id
        |               join alefdw.dim_grade dg on st.grade_uuid = dg.grade_id
        |               join alefdw.dim_tenant dt on st.tenant_uuid = dt.tenant_id
        |               join alefdw.dim_school ds on st.school_uuid = ds.school_id
        |               left join alefdw.dim_section dc on st.section_uuid = dc.section_id
        |               left join alefdw.dim_academic_year day on st.academic_year_uuid = day.academic_year_id
        |               left join alefdw.dim_class dcls on st.class_uuid = dcls.class_id and dcls.class_status = 1
        |      where st.ktg_session_is_start = true
        |        and st.ktg_session_is_start_event_processed = false
        |        and st.ktg_session_event_type = 1
        |        and (
        |         ((st.subject_uuid isnull and dsu.subject_dw_id isnull) or (st.subject_uuid notnull and dsu.subject_dw_id notnull)) and
        |         ((st.section_uuid isnull and dc.section_dw_id isnull) or (st.section_uuid notnull and dc.section_dw_id notnull)) and
        |         ((st.class_uuid isnull and dcls.class_dw_id isnull) or (st.class_uuid notnull and dcls.class_dw_id notnull)) and
        |         ((st.academic_year_uuid isnull and day.academic_year_dw_id isnull) or (st.academic_year_uuid notnull and day.academic_year_dw_id notnull))
        |        )
        |      union all
        |      select st.ktg_session_staging_id                         as start_id,
        |             st.ktg_session_id                                 as ktg_session_id,
        |             st.ktg_session_created_time                       as ktg_session_start_time,
        |             cast(null as timestamp)                           as ktg_session_end_time,
        |             getdate()                                         as ktg_session_dw_created_time,
        |             st.ktg_session_date_dw_id                         as ktg_session_date_dw_id,
        |             st.ktg_session_question_id                        as ktg_session_question_id,
        |             st.ktg_session_kt_id                              as ktg_session_kt_id,
        |             dt.tenant_dw_id                                   as ktg_session_tenant_dw_id,
        |             ru.user_dw_id                                     as ktg_session_student_dw_id,
        |             dsu.subject_dw_id                                 as ktg_session_subject_dw_id,
        |             ds.school_dw_id                                   as ktg_session_school_dw_id,
        |             dg.grade_dw_id                                    as ktg_session_grade_dw_id,
        |             dc.section_dw_id                                  as ktg_session_section_dw_id,
        |             day.academic_year_dw_id                           as ktg_session_academic_year_dw_id,
        |             cast(st.ktg_session_outside_of_school as boolean) as ktg_session_outside_of_school,
        |             st.ktg_session_trimester_id                       as ktg_session_trimester_id,
        |             st.ktg_session_trimester_order                    as ktg_session_trimester_order,
        |             st.ktg_session_type                               as ktg_session_type,
        |             st.ktg_session_question_time_allotted             as ktg_session_question_time_allotted,
        |             cast(null as int)                                 as ktg_session_time_spent,
        |             st.ktg_session_answer                             as ktg_session_answer,
        |             st.ktg_session_num_attempts                       as ktg_session_num_attempts,
        |             st.ktg_session_score                              as ktg_session_score,
        |             st.ktg_session_max_score                          as ktg_session_max_score,
        |             st.ktg_session_stars                              as ktg_session_stars,
        |             st.ktg_session_is_attended                        as ktg_session_is_attended,
        |             st.ktg_session_event_type                         as ktg_session_event_type,
        |             st.ktg_session_is_start                           as ktg_session_is_start,
        |             st.ktg_session_instructional_plan_id              as ktg_session_instructional_plan_id,
        |             st.ktg_session_learning_path_id                   as ktg_session_learning_path_id,
        |             dcls.class_dw_id                                  as ktg_session_session_class_dw_id,
        |             st.ktg_session_material_id                        as ktg_session_material_id,
        |             st.ktg_session_material_type                      as ktg_session_material_type,
        |             dlo.lo_dw_id                                      as ktg_session_lo_dw_id
        |      from alefdw_stage.staging_ktg_session st
        |               join alefdw_stage.rel_user ru on st.student_uuid = ru.user_id
        |               left join alefdw.dim_subject dsu on st.subject_uuid = dsu.subject_id
        |               join alefdw.dim_grade dg on st.grade_uuid = dg.grade_id
        |               join alefdw.dim_tenant dt on st.tenant_uuid = dt.tenant_id
        |               join alefdw.dim_school ds on st.school_uuid = ds.school_id
        |               left join alefdw.dim_section dc on st.section_uuid = dc.section_id
        |               left join alefdw.dim_academic_year day on st.academic_year_uuid = day.academic_year_id
        |               left join alefdw.dim_class dcls on st.class_uuid = dcls.class_id and dcls.class_status = 1
        |               join alefdw.dim_learning_objective dlo on st.lo_uuid = dlo.lo_id
        |      where st.ktg_session_is_start = true
        |        and st.ktg_session_is_start_event_processed = false
        |        and st.ktg_session_event_type = 2
        |        and (
        |         ((st.subject_uuid isnull and dsu.subject_dw_id isnull) or (st.subject_uuid notnull and dsu.subject_dw_id notnull)) and
        |         ((st.section_uuid isnull and dc.section_dw_id isnull) or (st.section_uuid notnull and dc.section_dw_id notnull)) and
        |         ((st.class_uuid isnull and dcls.class_dw_id isnull) or (st.class_uuid notnull and dcls.class_dw_id notnull)) and
        |         ((st.academic_year_uuid isnull and day.academic_year_dw_id isnull) or (st.academic_year_uuid notnull and day.academic_year_dw_id notnull))
        |        )) as a
        |where a.start_id in (1001, 1002, 1003)
        |order by a.start_id
        |limit 60000;
        |""".stripMargin)


    replaceSpecChars(sqlStringData.idSelectForDelete) shouldBe replaceSpecChars(
      """
        |select a.start_id, a.end_id
        |from (select st.ktg_session_staging_id                                                  as start_id,
        |             en.ktg_session_staging_id                                                  as end_id,
        |             st.ktg_session_id                                                          as ktg_session_id,
        |             st.ktg_session_created_time                                                as ktg_session_start_time,
        |             en.ktg_session_created_time                                                as ktg_session_end_time,
        |             getdate()                                                                  as ktg_session_dw_created_time,
        |             en.ktg_session_date_dw_id                                                  as ktg_session_date_dw_id,
        |             en.ktg_session_question_id                                                 as ktg_session_question_id,
        |             en.ktg_session_kt_id                                                       as ktg_session_kt_id,
        |             dt.tenant_dw_id                                                            as ktg_session_tenant_dw_id,
        |             ru.user_dw_id                                                              as ktg_session_student_dw_id,
        |             dsu.subject_dw_id                                                          as ktg_session_subject_dw_id,
        |             ds.school_dw_id                                                            as ktg_session_school_dw_id,
        |             dg.grade_dw_id                                                             as ktg_session_grade_dw_id,
        |             dc.section_dw_id                                                           as ktg_session_section_dw_id,
        |             day.academic_year_dw_id                                                    as ktg_session_academic_year_dw_id,
        |             cast(en.ktg_session_outside_of_school as boolean)                          as ktg_session_outside_of_school,
        |             en.ktg_session_trimester_id                                                as ktg_session_trimester_id,
        |             en.ktg_session_trimester_order                                             as ktg_session_trimester_order,
        |             en.ktg_session_type                                                        as ktg_session_type,
        |             en.ktg_session_question_time_allotted                                      as ktg_session_question_time_allotted,
        |             DATEDIFF(second, st.ktg_session_created_time, en.ktg_session_created_time) as ktg_session_time_spent,
        |             en.ktg_session_answer                                                      as ktg_session_answer,
        |             en.ktg_session_num_attempts                                                as ktg_session_num_attempts,
        |             en.ktg_session_score                                                       as ktg_session_score,
        |             en.ktg_session_max_score                                                   as ktg_session_max_score,
        |             en.ktg_session_stars                                                       as ktg_session_stars,
        |             en.ktg_session_is_attended                                                 as ktg_session_is_attended,
        |             en.ktg_session_event_type                                                  as ktg_session_event_type,
        |             en.ktg_session_is_start                                                    as ktg_session_is_start,
        |             en.ktg_session_instructional_plan_id                                       as ktg_session_instructional_plan_id,
        |             en.ktg_session_learning_path_id                                            as ktg_session_learning_path_id,
        |             dcls.class_dw_id                                                           as ktg_session_session_class_dw_id,
        |             en.ktg_session_material_id                                                 as ktg_session_material_id,
        |             en.ktg_session_material_type                                               as ktg_session_material_type,
        |             cast(null as bigint)                                                       as ktg_session_lo_dw_id
        |      from alefdw_stage.staging_ktg_session st
        |               join alefdw_stage.staging_ktg_session en on st.ktg_session_id = en.ktg_session_id and
        |                                                           st.ktg_session_event_type = en.ktg_session_event_type and
        |                                                           st.ktg_session_is_start = true and
        |                                                           en.ktg_session_is_start = false
        |               join alefdw_stage.rel_user ru on st.student_uuid = ru.user_id
        |               left join alefdw.dim_subject dsu on st.subject_uuid = dsu.subject_id
        |               join alefdw.dim_grade dg on st.grade_uuid = dg.grade_id
        |               join alefdw.dim_tenant dt on st.tenant_uuid = dt.tenant_id
        |               join alefdw.dim_school ds on st.school_uuid = ds.school_id
        |               left join alefdw.dim_section dc on st.section_uuid = dc.section_id
        |               left join alefdw.dim_academic_year day on st.academic_year_uuid = day.academic_year_id
        |               left join alefdw.dim_class dcls on st.class_uuid = dcls.class_id and dcls.class_status = 1
        |      where st.ktg_session_event_type = 1
        |        and (
        |         ((st.subject_uuid isnull and dsu.subject_dw_id isnull) or (st.subject_uuid notnull and dsu.subject_dw_id notnull)) and
        |         ((st.section_uuid isnull and dc.section_dw_id isnull) or (st.section_uuid notnull and dc.section_dw_id notnull)) and
        |         ((st.class_uuid isnull and dcls.class_dw_id isnull) or (st.class_uuid notnull and dcls.class_dw_id notnull)) and
        |         ((st.academic_year_uuid isnull and day.academic_year_dw_id isnull) or (st.academic_year_uuid notnull and day.academic_year_dw_id notnull))
        |        )
        |      union all
        |      select st.ktg_session_staging_id                                                  as start_id,
        |             en.ktg_session_staging_id                                                  as end_id,
        |             st.ktg_session_id                                                          as ktg_session_id,
        |             st.ktg_session_created_time                                                as ktg_session_start_time,
        |             en.ktg_session_created_time                                                as ktg_session_end_time,
        |             getdate()                                                                  as ktg_session_dw_created_time,
        |             en.ktg_session_date_dw_id                                                  as ktg_session_date_dw_id,
        |             en.ktg_session_question_id                                                 as ktg_session_question_id,
        |             en.ktg_session_kt_id                                                       as ktg_session_kt_id,
        |             dt.tenant_dw_id                                                            as ktg_session_tenant_dw_id,
        |             ru.user_dw_id                                                              as ktg_session_student_dw_id,
        |             dsu.subject_dw_id                                                          as ktg_session_subject_dw_id,
        |             ds.school_dw_id                                                            as ktg_session_school_dw_id,
        |             dg.grade_dw_id                                                             as ktg_session_grade_dw_id,
        |             dc.section_dw_id                                                           as ktg_session_section_dw_id,
        |             day.academic_year_dw_id                                                    as ktg_session_academic_year_dw_id,
        |             cast(en.ktg_session_outside_of_school as boolean)                          as ktg_session_outside_of_school,
        |             en.ktg_session_trimester_id                                                as ktg_session_trimester_id,
        |             en.ktg_session_trimester_order                                             as ktg_session_trimester_order,
        |             en.ktg_session_type                                                        as ktg_session_type,
        |             en.ktg_session_question_time_allotted                                      as ktg_session_question_time_allotted,
        |             DATEDIFF(second, st.ktg_session_created_time, en.ktg_session_created_time) as ktg_session_time_spent,
        |             en.ktg_session_answer                                                      as ktg_session_answer,
        |             en.ktg_session_num_attempts                                                as ktg_session_num_attempts,
        |             en.ktg_session_score                                                       as ktg_session_score,
        |             en.ktg_session_max_score                                                   as ktg_session_max_score,
        |             en.ktg_session_stars                                                       as ktg_session_stars,
        |             en.ktg_session_is_attended                                                 as ktg_session_is_attended,
        |             en.ktg_session_event_type                                                  as ktg_session_event_type,
        |             en.ktg_session_is_start                                                    as ktg_session_is_start,
        |             en.ktg_session_instructional_plan_id                                       as ktg_session_instructional_plan_id,
        |             en.ktg_session_learning_path_id                                            as ktg_session_learning_path_id,
        |             dcls.class_dw_id                                                           as ktg_session_session_class_dw_id,
        |             en.ktg_session_material_id                                                 as ktg_session_material_id,
        |             en.ktg_session_material_type                                               as ktg_session_material_type,
        |             dlo.lo_dw_id                                                               as ktg_session_lo_dw_id
        |      from alefdw_stage.staging_ktg_session st
        |               join alefdw_stage.staging_ktg_session en on st.ktg_session_id = en.ktg_session_id and
        |                                                           st.ktg_session_event_type = en.ktg_session_event_type and
        |                                                           st.ktg_session_is_start = true and
        |                                                           en.ktg_session_is_start = false and
        |                                                           st.ktg_session_question_id = en.ktg_session_question_id
        |               join alefdw_stage.rel_user ru on st.student_uuid = ru.user_id
        |               left join alefdw.dim_subject dsu on st.subject_uuid = dsu.subject_id
        |               join alefdw.dim_grade dg on st.grade_uuid = dg.grade_id
        |               join alefdw.dim_tenant dt on st.tenant_uuid = dt.tenant_id
        |               join alefdw.dim_school ds on st.school_uuid = ds.school_id
        |               left join alefdw.dim_section dc on st.section_uuid = dc.section_id
        |               left join alefdw.dim_academic_year day on st.academic_year_uuid = day.academic_year_id
        |               left join alefdw.dim_class dcls on st.class_uuid = dcls.class_id and dcls.class_status = 1
        |               join alefdw.dim_learning_objective dlo on en.lo_uuid = dlo.lo_id
        |      where st.ktg_session_event_type = 2
        |        and (
        |         ((st.subject_uuid isnull and dsu.subject_dw_id isnull) or (st.subject_uuid notnull and dsu.subject_dw_id notnull)) and
        |         ((st.section_uuid isnull and dc.section_dw_id isnull) or (st.section_uuid notnull and dc.section_dw_id notnull)) and
        |         ((st.class_uuid isnull and dcls.class_dw_id isnull) or (st.class_uuid notnull and dcls.class_dw_id notnull)) and
        |         ((st.academic_year_uuid isnull and day.academic_year_dw_id isnull) or (st.academic_year_uuid notnull and day.academic_year_dw_id notnull))
        |        )) as a
        |order by a.start_id
        |limit 60000;
        |""".stripMargin)


    replaceSpecChars(sqlStringData.idSelectForUpdate) shouldBe replaceSpecChars(
      """
        |select a.start_id
        |from (select st.ktg_session_staging_id                         as start_id,
        |             st.ktg_session_id                                 as ktg_session_id,
        |             st.ktg_session_created_time                       as ktg_session_start_time,
        |             cast(null as timestamp)                           as ktg_session_end_time,
        |             getdate()                                         as ktg_session_dw_created_time,
        |             st.ktg_session_date_dw_id                         as ktg_session_date_dw_id,
        |             st.ktg_session_question_id                        as ktg_session_question_id,
        |             st.ktg_session_kt_id                              as ktg_session_kt_id,
        |             dt.tenant_dw_id                                   as ktg_session_tenant_dw_id,
        |             ru.user_dw_id                                     as ktg_session_student_dw_id,
        |             dsu.subject_dw_id                                 as ktg_session_subject_dw_id,
        |             ds.school_dw_id                                   as ktg_session_school_dw_id,
        |             dg.grade_dw_id                                    as ktg_session_grade_dw_id,
        |             dc.section_dw_id                                  as ktg_session_section_dw_id,
        |             day.academic_year_dw_id                           as ktg_session_academic_year_dw_id,
        |             cast(st.ktg_session_outside_of_school as boolean) as ktg_session_outside_of_school,
        |             st.ktg_session_trimester_id                       as ktg_session_trimester_id,
        |             st.ktg_session_trimester_order                    as ktg_session_trimester_order,
        |             st.ktg_session_type                               as ktg_session_type,
        |             st.ktg_session_question_time_allotted             as ktg_session_question_time_allotted,
        |             cast(null as int)                                 as ktg_session_time_spent,
        |             st.ktg_session_answer                             as ktg_session_answer,
        |             st.ktg_session_num_attempts                       as ktg_session_num_attempts,
        |             st.ktg_session_score                              as ktg_session_score,
        |             st.ktg_session_max_score                          as ktg_session_max_score,
        |             st.ktg_session_stars                              as ktg_session_stars,
        |             st.ktg_session_is_attended                        as ktg_session_is_attended,
        |             st.ktg_session_event_type                         as ktg_session_event_type,
        |             st.ktg_session_is_start                           as ktg_session_is_start,
        |             st.ktg_session_instructional_plan_id              as ktg_session_instructional_plan_id,
        |             st.ktg_session_learning_path_id                   as ktg_session_learning_path_id,
        |             dcls.class_dw_id                                  as ktg_session_session_class_dw_id,
        |             st.ktg_session_material_id                        as ktg_session_material_id,
        |             st.ktg_session_material_type                      as ktg_session_material_type,
        |             cast(null as bigint)                              as ktg_session_lo_dw_id
        |      from alefdw_stage.staging_ktg_session st
        |               join alefdw_stage.rel_user ru on st.student_uuid = ru.user_id
        |               left join alefdw.dim_subject dsu on st.subject_uuid = dsu.subject_id
        |               join alefdw.dim_grade dg on st.grade_uuid = dg.grade_id
        |               join alefdw.dim_tenant dt on st.tenant_uuid = dt.tenant_id
        |               join alefdw.dim_school ds on st.school_uuid = ds.school_id
        |               left join alefdw.dim_section dc on st.section_uuid = dc.section_id
        |               left join alefdw.dim_academic_year day on st.academic_year_uuid = day.academic_year_id
        |               left join alefdw.dim_class dcls on st.class_uuid = dcls.class_id and dcls.class_status = 1
        |      where st.ktg_session_is_start = true
        |        and st.ktg_session_is_start_event_processed = false
        |        and st.ktg_session_event_type = 1
        |        and (
        |         ((st.subject_uuid isnull and dsu.subject_dw_id isnull) or (st.subject_uuid notnull and dsu.subject_dw_id notnull)) and
        |         ((st.section_uuid isnull and dc.section_dw_id isnull) or (st.section_uuid notnull and dc.section_dw_id notnull)) and
        |         ((st.class_uuid isnull and dcls.class_dw_id isnull) or (st.class_uuid notnull and dcls.class_dw_id notnull)) and
        |         ((st.academic_year_uuid isnull and day.academic_year_dw_id isnull) or (st.academic_year_uuid notnull and day.academic_year_dw_id notnull))
        |        )
        |      union all
        |      select st.ktg_session_staging_id                         as start_id,
        |             st.ktg_session_id                                 as ktg_session_id,
        |             st.ktg_session_created_time                       as ktg_session_start_time,
        |             cast(null as timestamp)                           as ktg_session_end_time,
        |             getdate()                                         as ktg_session_dw_created_time,
        |             st.ktg_session_date_dw_id                         as ktg_session_date_dw_id,
        |             st.ktg_session_question_id                        as ktg_session_question_id,
        |             st.ktg_session_kt_id                              as ktg_session_kt_id,
        |             dt.tenant_dw_id                                   as ktg_session_tenant_dw_id,
        |             ru.user_dw_id                                     as ktg_session_student_dw_id,
        |             dsu.subject_dw_id                                 as ktg_session_subject_dw_id,
        |             ds.school_dw_id                                   as ktg_session_school_dw_id,
        |             dg.grade_dw_id                                    as ktg_session_grade_dw_id,
        |             dc.section_dw_id                                  as ktg_session_section_dw_id,
        |             day.academic_year_dw_id                           as ktg_session_academic_year_dw_id,
        |             cast(st.ktg_session_outside_of_school as boolean) as ktg_session_outside_of_school,
        |             st.ktg_session_trimester_id                       as ktg_session_trimester_id,
        |             st.ktg_session_trimester_order                    as ktg_session_trimester_order,
        |             st.ktg_session_type                               as ktg_session_type,
        |             st.ktg_session_question_time_allotted             as ktg_session_question_time_allotted,
        |             cast(null as int)                                 as ktg_session_time_spent,
        |             st.ktg_session_answer                             as ktg_session_answer,
        |             st.ktg_session_num_attempts                       as ktg_session_num_attempts,
        |             st.ktg_session_score                              as ktg_session_score,
        |             st.ktg_session_max_score                          as ktg_session_max_score,
        |             st.ktg_session_stars                              as ktg_session_stars,
        |             st.ktg_session_is_attended                        as ktg_session_is_attended,
        |             st.ktg_session_event_type                         as ktg_session_event_type,
        |             st.ktg_session_is_start                           as ktg_session_is_start,
        |             st.ktg_session_instructional_plan_id              as ktg_session_instructional_plan_id,
        |             st.ktg_session_learning_path_id                   as ktg_session_learning_path_id,
        |             dcls.class_dw_id                                  as ktg_session_session_class_dw_id,
        |             st.ktg_session_material_id                        as ktg_session_material_id,
        |             st.ktg_session_material_type                      as ktg_session_material_type,
        |             dlo.lo_dw_id                                      as ktg_session_lo_dw_id
        |      from alefdw_stage.staging_ktg_session st
        |               join alefdw_stage.rel_user ru on st.student_uuid = ru.user_id
        |               left join alefdw.dim_subject dsu on st.subject_uuid = dsu.subject_id
        |               join alefdw.dim_grade dg on st.grade_uuid = dg.grade_id
        |               join alefdw.dim_tenant dt on st.tenant_uuid = dt.tenant_id
        |               join alefdw.dim_school ds on st.school_uuid = ds.school_id
        |               left join alefdw.dim_section dc on st.section_uuid = dc.section_id
        |               left join alefdw.dim_academic_year day on st.academic_year_uuid = day.academic_year_id
        |               left join alefdw.dim_class dcls on st.class_uuid = dcls.class_id and dcls.class_status = 1
        |               join alefdw.dim_learning_objective dlo on st.lo_uuid = dlo.lo_id
        |      where st.ktg_session_is_start = true
        |        and st.ktg_session_is_start_event_processed = false
        |        and st.ktg_session_event_type = 2
        |        and (
        |         ((st.subject_uuid isnull and dsu.subject_dw_id isnull) or (st.subject_uuid notnull and dsu.subject_dw_id notnull)) and
        |         ((st.section_uuid isnull and dc.section_dw_id isnull) or (st.section_uuid notnull and dc.section_dw_id notnull)) and
        |         ((st.class_uuid isnull and dcls.class_dw_id isnull) or (st.class_uuid notnull and dcls.class_dw_id notnull)) and
        |         ((st.academic_year_uuid isnull and day.academic_year_dw_id isnull) or (st.academic_year_uuid notnull and day.academic_year_dw_id notnull))
        |       )) as a
        |order by a.start_id
        |limit 60000;
        |""".stripMargin)


    replaceSpecChars(sqlStringData.deleteDanglingSql) shouldBe replaceSpecChars(
      """
        |DELETE
        |FROM alefdw_stage.staging_ktg_session USING alefdw.dim_academic_year ay
        |WHERE staging_ktg_session.academic_year_uuid = ay.academic_year_id
        |  AND ay.academic_year_status = 1
        |  AND ay.academic_year_end_date < current_date
        |  AND staging_ktg_session.ktg_session_is_start = true
        |  AND staging_ktg_session.ktg_session_is_start_event_processed = true;
        |""".stripMargin)
  }
}
