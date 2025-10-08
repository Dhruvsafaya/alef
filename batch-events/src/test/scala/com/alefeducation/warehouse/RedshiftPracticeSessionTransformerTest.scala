package com.alefeducation.warehouse

import com.alefeducation.util.StringUtilities.replaceSpecChars
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class RedshiftPracticeSessionTransformerTest extends AnyFunSuite with Matchers {

  val transformer: RedshiftPracticeSessionTransformer.type = RedshiftPracticeSessionTransformer

  val factCols: String =
    """practice_session_start_time,
      |practice_session_end_time,
      |practice_session_dw_created_time,
      |practice_session_date_dw_id,
      |practice_session_id,
      |practice_session_lo_dw_id,
      |practice_session_student_dw_id,
      |practice_session_subject_dw_id,
      |practice_session_grade_dw_id,
      |practice_session_tenant_dw_id,
      |practice_session_school_dw_id,
      |practice_session_section_dw_id,
      |practice_session_sa_score,
      |practice_session_item_lo_dw_id,
      |practice_session_item_content_uuid,
      |practice_session_item_content_title,
      |practice_session_item_content_lesson_type,
      |practice_session_item_content_location,
      |practice_session_time_spent,
      |practice_session_score,
      |practice_session_event_type,
      |practice_session_is_start,
      |practice_session_outside_of_school,
      |practice_session_stars,
      |practice_session_class_dw_id,
      |practice_session_item_step_id,
      |practice_session_learning_path_id,
      |practice_session_academic_year_dw_id,
      |practice_session_instructional_plan_id,
      |practice_session_material_id,
      |practice_session_material_type""".stripMargin

  test("Practice session should prepare queries for transform data from staging table to main") {
    val sqlStringData = transformer.prepareSqlStrings("alefdw", factCols)

    transformer.factEntity shouldBe "practice"

    replaceSpecChars(sqlStringData.insertSql("1001, 1002, 1003")) shouldBe replaceSpecChars(
      """
        |insert into alefdw.fact_practice_session(practice_session_start_time, practice_session_end_time,
        |                                         practice_session_dw_created_time, practice_session_date_dw_id,
        |                                         practice_session_id, practice_session_lo_dw_id, practice_session_student_dw_id,
        |                                         practice_session_subject_dw_id, practice_session_grade_dw_id,
        |                                         practice_session_tenant_dw_id, practice_session_school_dw_id,
        |                                         practice_session_section_dw_id, practice_session_sa_score,
        |                                         practice_session_item_lo_dw_id, practice_session_item_content_uuid,
        |                                         practice_session_item_content_title, practice_session_item_content_lesson_type,
        |                                         practice_session_item_content_location, practice_session_time_spent,
        |                                         practice_session_score, practice_session_event_type, practice_session_is_start,
        |                                         practice_session_outside_of_school, practice_session_stars,
        |                                         practice_session_class_dw_id, practice_session_item_step_id,
        |                                         practice_session_learning_path_id, practice_session_academic_year_dw_id,
        |                                         practice_session_instructional_plan_id, practice_session_material_id,
        |                                         practice_session_material_type)
        |select a.practice_session_start_time,
        |       a.practice_session_end_time,
        |       a.practice_session_dw_created_time,
        |       a.practice_session_date_dw_id,
        |       a.practice_session_id,
        |       a.practice_session_lo_dw_id,
        |       a.practice_session_student_dw_id,
        |       a.practice_session_subject_dw_id,
        |       a.practice_session_grade_dw_id,
        |       a.practice_session_tenant_dw_id,
        |       a.practice_session_school_dw_id,
        |       a.practice_session_section_dw_id,
        |       a.practice_session_sa_score,
        |       a.practice_session_item_lo_dw_id,
        |       a.practice_session_item_content_uuid,
        |       a.practice_session_item_content_title,
        |       a.practice_session_item_content_lesson_type,
        |       a.practice_session_item_content_location,
        |       a.practice_session_time_spent,
        |       a.practice_session_score,
        |       a.practice_session_event_type,
        |       a.practice_session_is_start,
        |       a.practice_session_outside_of_school,
        |       a.practice_session_stars,
        |       a.practice_session_academic_year_dw_id,
        |       a.practice_session_instructional_plan_id,
        |       a.practice_session_learning_path_id,
        |       a.practice_session_class_dw_id,
        |       a.practice_session_item_step_id,
        |       a.practice_session_material_id,
        |       a.practice_session_material_type
        |from (select st.practice_session_staging_id                                                       as start_id,
        |             en.practice_session_staging_id                                                       as end_id,
        |             st.practice_session_created_time                                                     as practice_session_start_time,
        |             en.practice_session_created_time                                                     as practice_session_end_time,
        |             getdate()                                                                            as practice_session_dw_created_time,
        |             en.practice_session_date_dw_id                                                       as practice_session_date_dw_id,
        |             st.practice_session_id                                                               as practice_session_id,
        |             dlo.lo_dw_id                                                                         as practice_session_lo_dw_id,
        |             ru.user_dw_id                                                                        as practice_session_student_dw_id,
        |             dsu.subject_dw_id                                                                    as practice_session_subject_dw_id,
        |             dg.grade_dw_id                                                                       as practice_session_grade_dw_id,
        |             dt.tenant_dw_id                                                                      as practice_session_tenant_dw_id,
        |             ds.school_dw_id                                                                      as practice_session_school_dw_id,
        |             dsec.section_dw_id                                                                   as practice_session_section_dw_id,
        |             st.practice_session_item_content_uuid                                                as practice_session_item_content_uuid,
        |             st.practice_session_item_content_title                                               as practice_session_item_content_title,
        |             st.practice_session_item_content_lesson_type                                         as practice_session_item_content_lesson_type,
        |             st.practice_session_item_content_location                                            as practice_session_item_content_location,
        |             st.practice_session_sa_score                                                         as practice_session_sa_score,
        |             en.practice_session_score                                                            as practice_session_score,
        |             st.practice_session_event_type                                                       as practice_session_event_type,
        |             DATEDIFF(second, st.practice_session_created_time,
        |                      en.practice_session_created_time)                                           as practice_session_time_spent,
        |             en.practice_session_is_start                                                         as practice_session_is_start,
        |             en.practice_session_outside_of_school                                                as practice_session_outside_of_school,
        |             en.practice_session_stars                                                            as practice_session_stars,
        |             day.academic_year_dw_id                                                              as practice_session_academic_year_dw_id,
        |             en.practice_session_instructional_plan_id                                            as practice_session_instructional_plan_id,
        |             en.practice_session_learning_path_id                                                 as practice_session_learning_path_id,
        |             dcls.class_dw_id                                                                     as practice_session_class_dw_id,
        |             en.practice_session_material_id                                                      as practice_session_material_id,
        |             en.practice_session_material_type                                                    as practice_session_material_type,
        |             null                                                                                 as practice_session_item_lo_dw_id,
        |             en.practice_session_item_step_id                                                     as practice_session_item_step_id
        |      from alefdw_stage.staging_practice_session st
        |               join alefdw_stage.staging_practice_session en on st.practice_session_id = en.practice_session_id and
        |                                                                st.practice_session_event_type =
        |                                                                en.practice_session_event_type and
        |                                                                st.practice_session_is_start = true and
        |                                                                en.practice_session_is_start = false
        |               join alefdw.dim_learning_objective dlo on st.lo_uuid = dlo.lo_id
        |               join alefdw_stage.rel_user ru on st.student_uuid = ru.user_id
        |               left join alefdw.dim_subject dsu on st.subject_uuid = dsu.subject_id
        |               join alefdw.dim_grade dg on st.grade_uuid = dg.grade_id
        |               join alefdw.dim_tenant dt on st.tenant_uuid = dt.tenant_id
        |               join alefdw.dim_school ds on st.school_uuid = ds.school_id
        |               left join alefdw.dim_section dsec on st.section_uuid = dsec.section_id
        |               left join alefdw.dim_academic_year day on st.academic_year_uuid = day.academic_year_id
        |               left join alefdw.dim_class dcls on st.class_uuid = dcls.class_id and dcls.class_status = 1
        |      where st.practice_session_event_type = 1
        |        and (
        |         ((st.subject_uuid isnull and dsu.subject_dw_id isnull) or (st.subject_uuid notnull and dsu.subject_dw_id notnull)) and
        |         ((st.section_uuid isnull and dsec.section_dw_id isnull) or (st.section_uuid notnull and dsec.section_dw_id notnull)) and
        |         ((st.class_uuid isnull and dcls.class_dw_id isnull) or (st.class_uuid notnull and dcls.class_dw_id notnull)) and
        |         ((st.academic_year_uuid isnull and day.academic_year_dw_id isnull) or (st.academic_year_uuid notnull and day.academic_year_dw_id notnull))
        |        )
        |      union all
        |      select st.practice_session_staging_id                                                       as start_id,
        |             en.practice_session_staging_id                                                       as end_id,
        |             st.practice_session_created_time                                                     as practice_session_start_time,
        |             en.practice_session_created_time                                                     as practice_session_end_time,
        |             getdate()                                                                            as practice_session_dw_created_time,
        |             en.practice_session_date_dw_id                                                       as practice_session_date_dw_id,
        |             st.practice_session_id                                                               as practice_session_id,
        |             dlo.lo_dw_id                                                                         as practice_session_lo_dw_id,
        |             ru.user_dw_id                                                                        as practice_session_student_dw_id,
        |             dsu.subject_dw_id                                                                    as practice_session_subject_dw_id,
        |             dg.grade_dw_id                                                                       as practice_session_grade_dw_id,
        |             dt.tenant_dw_id                                                                      as practice_session_tenant_dw_id,
        |             ds.school_dw_id                                                                      as practice_session_school_dw_id,
        |             dsec.section_dw_id                                                                   as practice_session_section_dw_id,
        |             st.practice_session_item_content_uuid                                                as practice_session_item_content_uuid,
        |             st.practice_session_item_content_title                                               as practice_session_item_content_title,
        |             st.practice_session_item_content_lesson_type                                         as practice_session_item_content_lesson_type,
        |             st.practice_session_item_content_location                                            as practice_session_item_content_location,
        |             st.practice_session_sa_score                                                         as practice_session_sa_score,
        |             en.practice_session_score                                                            as practice_session_score,
        |             st.practice_session_event_type                                                       as practice_session_event_type,
        |             DATEDIFF(second, st.practice_session_created_time,
        |                      en.practice_session_created_time)                                           as practice_session_time_spent,
        |             en.practice_session_is_start                                                         as practice_session_is_start,
        |             en.practice_session_outside_of_school                                                as practice_session_outside_of_school,
        |             en.practice_session_stars                                                            as practice_session_stars,
        |             day.academic_year_dw_id                                                              as practice_session_academic_year_dw_id,
        |             en.practice_session_instructional_plan_id                                            as practice_session_instructional_plan_id,
        |             en.practice_session_learning_path_id                                                 as practice_session_learning_path_id,
        |             dcls.class_dw_id                                                                     as practice_session_class_dw_id,
        |             en.practice_session_material_id                                                      as practice_session_material_id,
        |             en.practice_session_material_type                                                    as practice_session_material_type,
        |             dlo1.lo_dw_id                                                                        as practice_session_item_lo_dw_id,
        |             en.practice_session_item_step_id                                                     as practice_session_item_step_id
        |      from alefdw_stage.staging_practice_session st
        |               join alefdw_stage.staging_practice_session en on st.practice_session_id = en.practice_session_id and
        |                                                                st.practice_session_event_type =
        |                                                                en.practice_session_event_type and
        |                                                                st.practice_session_is_start = true and
        |                                                                en.practice_session_is_start = false and
        |                                                                st.practice_session_item_lo_uuid =
        |                                                                en.practice_session_item_lo_uuid
        |               join alefdw.dim_learning_objective dlo on st.lo_uuid = dlo.lo_id
        |               join alefdw_stage.rel_user ru on st.student_uuid = ru.user_id
        |               left join alefdw.dim_subject dsu on st.subject_uuid = dsu.subject_id
        |               join alefdw.dim_grade dg on st.grade_uuid = dg.grade_id
        |               join alefdw.dim_tenant dt on st.tenant_uuid = dt.tenant_id
        |               join alefdw.dim_school ds on st.school_uuid = ds.school_id
        |               left join alefdw.dim_section dsec on st.section_uuid = dsec.section_id
        |               left join alefdw.dim_academic_year day on st.academic_year_uuid = day.academic_year_id
        |               left join alefdw.dim_class dcls on st.class_uuid = dcls.class_id and dcls.class_status = 1
        |               join alefdw.dim_learning_objective dlo1 on st.practice_session_item_lo_uuid = dlo1.lo_id
        |      where st.practice_session_event_type = 2
        |        and (
        |         ((st.subject_uuid isnull and dsu.subject_dw_id isnull) or (st.subject_uuid notnull and dsu.subject_dw_id notnull)) and
        |         ((st.section_uuid isnull and dsec.section_dw_id isnull) or (st.section_uuid notnull and dsec.section_dw_id notnull)) and
        |         ((st.class_uuid isnull and dcls.class_dw_id isnull) or (st.class_uuid notnull and dcls.class_dw_id notnull)) and
        |         ((st.academic_year_uuid isnull and day.academic_year_dw_id isnull) or (st.academic_year_uuid notnull and day.academic_year_dw_id notnull))
        |        )
        |      union all
        |      select st.practice_session_staging_id                                                       as start_id,
        |             en.practice_session_staging_id                                                       as end_id,
        |             st.practice_session_created_time                                                     as practice_session_start_time,
        |             en.practice_session_created_time                                                     as practice_session_end_time,
        |             getdate()                                                                            as practice_session_dw_created_time,
        |             en.practice_session_date_dw_id                                                       as practice_session_date_dw_id,
        |             st.practice_session_id                                                               as practice_session_id,
        |             dlo.lo_dw_id                                                                         as practice_session_lo_dw_id,
        |             ru.user_dw_id                                                                        as practice_session_student_dw_id,
        |             dsu.subject_dw_id                                                                    as practice_session_subject_dw_id,
        |             dg.grade_dw_id                                                                       as practice_session_grade_dw_id,
        |             dt.tenant_dw_id                                                                      as practice_session_tenant_dw_id,
        |             ds.school_dw_id                                                                      as practice_session_school_dw_id,
        |             dsec.section_dw_id                                                                   as practice_session_section_dw_id,
        |             st.practice_session_item_content_uuid                                                as practice_session_item_content_uuid,
        |             st.practice_session_item_content_title                                               as practice_session_item_content_title,
        |             st.practice_session_item_content_lesson_type                                         as practice_session_item_content_lesson_type,
        |             st.practice_session_item_content_location                                            as practice_session_item_content_location,
        |             st.practice_session_sa_score                                                         as practice_session_sa_score,
        |             en.practice_session_score                                                            as practice_session_score,
        |             st.practice_session_event_type                                                       as practice_session_event_type,
        |             DATEDIFF(second, st.practice_session_created_time,
        |                      en.practice_session_created_time)                                           as practice_session_time_spent,
        |             en.practice_session_is_start                                                         as practice_session_is_start,
        |             en.practice_session_outside_of_school                                                as practice_session_outside_of_school,
        |             en.practice_session_stars                                                            as practice_session_stars,
        |             day.academic_year_dw_id                                                              as practice_session_academic_year_dw_id,
        |             en.practice_session_instructional_plan_id                                            as practice_session_instructional_plan_id,
        |             en.practice_session_learning_path_id                                                 as practice_session_learning_path_id,
        |             dcls.class_dw_id                                                                     as practice_session_class_dw_id,
        |             en.practice_session_material_id                                                      as practice_session_material_id,
        |             en.practice_session_material_type                                                    as practice_session_material_type,
        |             dlo1.lo_dw_id                                                                        as practice_session_item_lo_dw_id,
        |             en.practice_session_item_step_id                                                     as practice_session_item_step_id
        |      from alefdw_stage.staging_practice_session st
        |               join alefdw_stage.staging_practice_session en on st.practice_session_id = en.practice_session_id and
        |                                                                st.practice_session_event_type =
        |                                                                en.practice_session_event_type and
        |                                                                st.practice_session_is_start = true and
        |                                                                en.practice_session_is_start = false and
        |                                                                st.practice_session_item_lo_uuid =
        |                                                                en.practice_session_item_lo_uuid and
        |                                                                st.practice_session_item_step_id =
        |                                                                en.practice_session_item_step_id
        |               join alefdw.dim_learning_objective dlo on st.lo_uuid = dlo.lo_id
        |               join alefdw_stage.rel_user ru on st.student_uuid = ru.user_id
        |               left join alefdw.dim_subject dsu on st.subject_uuid = dsu.subject_id
        |               join alefdw.dim_grade dg on st.grade_uuid = dg.grade_id
        |               join alefdw.dim_tenant dt on st.tenant_uuid = dt.tenant_id
        |               join alefdw.dim_school ds on st.school_uuid = ds.school_id
        |               left join alefdw.dim_section dsec on st.section_uuid = dsec.section_id
        |               left join alefdw.dim_academic_year day on st.academic_year_uuid = day.academic_year_id
        |               left join alefdw.dim_class dcls on st.class_uuid = dcls.class_id and dcls.class_status = 1
        |               join alefdw.dim_learning_objective dlo1 on st.practice_session_item_lo_uuid = dlo1.lo_id
        |      where st.practice_session_event_type = 3
        |        and (
        |         ((st.subject_uuid isnull and dsu.subject_dw_id isnull) or (st.subject_uuid notnull and dsu.subject_dw_id notnull)) and
        |         ((st.section_uuid isnull and dsec.section_dw_id isnull) or (st.section_uuid notnull and dsec.section_dw_id notnull)) and
        |         ((st.class_uuid isnull and dcls.class_dw_id isnull) or (st.class_uuid notnull and dcls.class_dw_id notnull)) and
        |         ((st.academic_year_uuid isnull and day.academic_year_dw_id isnull) or (st.academic_year_uuid notnull and day.academic_year_dw_id notnull))
        |        )) as a
        |where (a.start_id in (1001, 1002, 1003) or a.end_id in (1001, 1002, 1003))
        |order by a.start_id
        |limit 60000;
        |""".stripMargin)


    replaceSpecChars(sqlStringData.startEventsInsert("1001, 1002, 1003")) shouldBe replaceSpecChars(
      """
        |insert into alefdw.fact_practice_session(practice_session_start_time, practice_session_end_time,
        |                                         practice_session_dw_created_time, practice_session_date_dw_id,
        |                                         practice_session_id, practice_session_lo_dw_id, practice_session_student_dw_id,
        |                                         practice_session_subject_dw_id, practice_session_grade_dw_id,
        |                                         practice_session_tenant_dw_id, practice_session_school_dw_id,
        |                                         practice_session_section_dw_id, practice_session_sa_score,
        |                                         practice_session_item_lo_dw_id, practice_session_item_content_uuid,
        |                                         practice_session_item_content_title, practice_session_item_content_lesson_type,
        |                                         practice_session_item_content_location, practice_session_time_spent,
        |                                         practice_session_score, practice_session_event_type, practice_session_is_start,
        |                                         practice_session_outside_of_school, practice_session_stars,
        |                                         practice_session_class_dw_id, practice_session_item_step_id,
        |                                         practice_session_learning_path_id, practice_session_academic_year_dw_id,
        |                                         practice_session_instructional_plan_id, practice_session_material_id,
        |                                         practice_session_material_type)
        |select a.practice_session_start_time,
        |       a.practice_session_end_time,
        |       a.practice_session_dw_created_time,
        |       a.practice_session_date_dw_id,
        |       a.practice_session_id,
        |       a.practice_session_lo_dw_id,
        |       a.practice_session_student_dw_id,
        |       a.practice_session_subject_dw_id,
        |       a.practice_session_grade_dw_id,
        |       a.practice_session_tenant_dw_id,
        |       a.practice_session_school_dw_id,
        |       a.practice_session_section_dw_id,
        |       a.practice_session_sa_score,
        |       a.practice_session_item_lo_dw_id,
        |       a.practice_session_item_content_uuid,
        |       a.practice_session_item_content_title,
        |       a.practice_session_item_content_lesson_type,
        |       a.practice_session_item_content_location,
        |       a.practice_session_time_spent,
        |       a.practice_session_score,
        |       a.practice_session_event_type,
        |       a.practice_session_is_start,
        |       a.practice_session_outside_of_school,
        |       a.practice_session_stars,
        |       a.practice_session_academic_year_dw_id,
        |       a.practice_session_instructional_plan_id,
        |       a.practice_session_learning_path_id,
        |       a.practice_session_class_dw_id,
        |       a.practice_session_item_step_id,
        |       a.practice_session_material_id,
        |       a.practice_session_material_type
        |from (select st.practice_session_staging_id               as start_id,
        |             st.practice_session_created_time             as practice_session_start_time,
        |             cast(null as timestamp)                      as practice_session_end_time,
        |             getdate()                                    as practice_session_dw_created_time,
        |             st.practice_session_date_dw_id               as practice_session_date_dw_id,
        |             st.practice_session_id                       as practice_session_id,
        |             dlo.lo_dw_id                                 as practice_session_lo_dw_id,
        |             ru.user_dw_id                                as practice_session_student_dw_id,
        |             dsu.subject_dw_id                            as practice_session_subject_dw_id,
        |             dg.grade_dw_id                               as practice_session_grade_dw_id,
        |             dt.tenant_dw_id                              as practice_session_tenant_dw_id,
        |             ds.school_dw_id                              as practice_session_school_dw_id,
        |             dsec.section_dw_id                           as practice_session_section_dw_id,
        |             st.practice_session_sa_score                 as practice_session_sa_score,
        |             st.practice_session_item_content_uuid        as practice_session_item_content_uuid,
        |             st.practice_session_item_content_title       as practice_session_item_content_title,
        |             st.practice_session_item_content_lesson_type as practice_session_item_content_lesson_type,
        |             st.practice_session_item_content_location    as practice_session_item_content_location,
        |             st.practice_session_event_type               as practice_session_event_type,
        |             cast(null as int)                            as practice_session_time_spent,
        |             cast(null as bigint)                         as practice_session_score,
        |             st.practice_session_is_start                 as practice_session_is_start,
        |             st.practice_session_outside_of_school        as practice_session_outside_of_school,
        |             st.practice_session_stars                    as practice_session_stars,
        |             day.academic_year_dw_id                      as practice_session_academic_year_dw_id,
        |             st.practice_session_instructional_plan_id    as practice_session_instructional_plan_id,
        |             st.practice_session_learning_path_id         as practice_session_learning_path_id,
        |             dcls.class_dw_id                             as practice_session_class_dw_id,
        |             st.practice_session_material_id              as practice_session_material_id,
        |             st.practice_session_material_type            as practice_session_material_type,
        |             null                                         as practice_session_item_lo_dw_id,
        |             st.practice_session_item_step_id             as practice_session_item_step_id
        |      from alefdw_stage.staging_practice_session st
        |               join alefdw.dim_learning_objective dlo on st.lo_uuid = dlo.lo_id
        |               join alefdw_stage.rel_user ru on st.student_uuid = ru.user_id
        |               left join alefdw.dim_subject dsu on st.subject_uuid = dsu.subject_id
        |               join alefdw.dim_grade dg on st.grade_uuid = dg.grade_id
        |               join alefdw.dim_tenant dt on st.tenant_uuid = dt.tenant_id
        |               join alefdw.dim_school ds on st.school_uuid = ds.school_id
        |               left join alefdw.dim_section dsec on st.section_uuid = dsec.section_id
        |               left join alefdw.dim_academic_year day on st.academic_year_uuid = day.academic_year_id
        |               left join alefdw.dim_class dcls on st.class_uuid = dcls.class_id and dcls.class_status = 1
        |      where st.practice_session_is_start = true
        |        and st.practice_session_is_start_event_processed = false
        |        and (
        |         ((st.subject_uuid isnull and dsu.subject_dw_id isnull) or (st.subject_uuid notnull and dsu.subject_dw_id notnull)) and
        |         ((st.section_uuid isnull and dsec.section_dw_id isnull) or (st.section_uuid notnull and dsec.section_dw_id notnull)) and
        |         ((st.class_uuid isnull and dcls.class_dw_id isnull) or (st.class_uuid notnull and dcls.class_dw_id notnull)) and
        |         ((st.academic_year_uuid isnull and day.academic_year_dw_id isnull) or (st.academic_year_uuid notnull and day.academic_year_dw_id notnull))
        |        )
        |        and st.practice_session_event_type = 1
        |      union all
        |      select st.practice_session_staging_id               as start_id,
        |             st.practice_session_created_time             as practice_session_start_time,
        |             cast(null as timestamp)                      as practice_session_end_time,
        |             getdate()                                    as practice_session_dw_created_time,
        |             st.practice_session_date_dw_id               as practice_session_date_dw_id,
        |             st.practice_session_id                       as practice_session_id,
        |             dlo.lo_dw_id                                 as practice_session_lo_dw_id,
        |             ru.user_dw_id                                as practice_session_student_dw_id,
        |             dsu.subject_dw_id                            as practice_session_subject_dw_id,
        |             dg.grade_dw_id                               as practice_session_grade_dw_id,
        |             dt.tenant_dw_id                              as practice_session_tenant_dw_id,
        |             ds.school_dw_id                              as practice_session_school_dw_id,
        |             dsec.section_dw_id                           as practice_session_section_dw_id,
        |             st.practice_session_sa_score                 as practice_session_sa_score,
        |             st.practice_session_item_content_uuid        as practice_session_item_content_uuid,
        |             st.practice_session_item_content_title       as practice_session_item_content_title,
        |             st.practice_session_item_content_lesson_type as practice_session_item_content_lesson_type,
        |             st.practice_session_item_content_location    as practice_session_item_content_location,
        |             st.practice_session_event_type               as practice_session_event_type,
        |             cast(null as int)                            as practice_session_time_spent,
        |             cast(null as bigint)                         as practice_session_score,
        |             st.practice_session_is_start                 as practice_session_is_start,
        |             st.practice_session_outside_of_school        as practice_session_outside_of_school,
        |             st.practice_session_stars                    as practice_session_stars,
        |             day.academic_year_dw_id                      as practice_session_academic_year_dw_id,
        |             st.practice_session_instructional_plan_id    as practice_session_instructional_plan_id,
        |             st.practice_session_learning_path_id         as practice_session_learning_path_id,
        |             dcls.class_dw_id                             as practice_session_class_dw_id,
        |             st.practice_session_material_id              as practice_session_material_id,
        |             st.practice_session_material_type            as practice_session_material_type,
        |             dlo1.lo_dw_id                                as practice_session_item_lo_dw_id,
        |             st.practice_session_item_step_id             as practice_session_item_step_id
        |      from alefdw_stage.staging_practice_session st
        |               join alefdw.dim_learning_objective dlo on st.lo_uuid = dlo.lo_id
        |               join alefdw_stage.rel_user ru on st.student_uuid = ru.user_id
        |               left join alefdw.dim_subject dsu on st.subject_uuid = dsu.subject_id
        |               join alefdw.dim_grade dg on st.grade_uuid = dg.grade_id
        |               join alefdw.dim_tenant dt on st.tenant_uuid = dt.tenant_id
        |               join alefdw.dim_school ds on st.school_uuid = ds.school_id
        |               left join alefdw.dim_section dsec on st.section_uuid = dsec.section_id
        |               left join alefdw.dim_academic_year day on st.academic_year_uuid = day.academic_year_id
        |               left join alefdw.dim_class dcls on st.class_uuid = dcls.class_id and dcls.class_status = 1
        |               join alefdw.dim_learning_objective dlo1 on st.practice_session_item_lo_uuid = dlo1.lo_id
        |      where st.practice_session_is_start = true
        |        and st.practice_session_is_start_event_processed = false
        |        and (
        |         ((st.subject_uuid isnull and dsu.subject_dw_id isnull) or (st.subject_uuid notnull and dsu.subject_dw_id notnull)) and
        |         ((st.section_uuid isnull and dsec.section_dw_id isnull) or (st.section_uuid notnull and dsec.section_dw_id notnull)) and
        |         ((st.class_uuid isnull and dcls.class_dw_id isnull) or (st.class_uuid notnull and dcls.class_dw_id notnull)) and
        |         ((st.academic_year_uuid isnull and day.academic_year_dw_id isnull) or (st.academic_year_uuid notnull and day.academic_year_dw_id notnull))
        |        )
        |        and st.practice_session_event_type IN (2, 3)) as a
        |where a.start_id in (1001, 1002, 1003)
        |order by a.start_id
        |limit 60000;
        |""".stripMargin
    )


    replaceSpecChars(sqlStringData.idSelectForUpdate) shouldBe replaceSpecChars(
      """
        |select a.start_id
        |from (select st.practice_session_staging_id               as start_id,
        |             st.practice_session_created_time             as practice_session_start_time,
        |             cast(null as timestamp)                      as practice_session_end_time,
        |             getdate()                                    as practice_session_dw_created_time,
        |             st.practice_session_date_dw_id               as practice_session_date_dw_id,
        |             st.practice_session_id                       as practice_session_id,
        |             dlo.lo_dw_id                                 as practice_session_lo_dw_id,
        |             ru.user_dw_id                                as practice_session_student_dw_id,
        |             dsu.subject_dw_id                            as practice_session_subject_dw_id,
        |             dg.grade_dw_id                               as practice_session_grade_dw_id,
        |             dt.tenant_dw_id                              as practice_session_tenant_dw_id,
        |             ds.school_dw_id                              as practice_session_school_dw_id,
        |             dsec.section_dw_id                           as practice_session_section_dw_id,
        |             st.practice_session_sa_score                 as practice_session_sa_score,
        |             st.practice_session_item_content_uuid        as practice_session_item_content_uuid,
        |             st.practice_session_item_content_title       as practice_session_item_content_title,
        |             st.practice_session_item_content_lesson_type as practice_session_item_content_lesson_type,
        |             st.practice_session_item_content_location    as practice_session_item_content_location,
        |             st.practice_session_event_type               as practice_session_event_type,
        |             cast(null as int)                            as practice_session_time_spent,
        |             cast(null as bigint)                         as practice_session_score,
        |             st.practice_session_is_start                 as practice_session_is_start,
        |             st.practice_session_outside_of_school        as practice_session_outside_of_school,
        |             st.practice_session_stars                    as practice_session_stars,
        |             day.academic_year_dw_id                      as practice_session_academic_year_dw_id,
        |             st.practice_session_instructional_plan_id    as practice_session_instructional_plan_id,
        |             st.practice_session_learning_path_id         as practice_session_learning_path_id,
        |             dcls.class_dw_id                             as practice_session_class_dw_id,
        |             st.practice_session_material_id              as practice_session_material_id,
        |             st.practice_session_material_type            as practice_session_material_type,
        |             null                                         as practice_session_item_lo_dw_id,
        |             st.practice_session_item_step_id             as practice_session_item_step_id
        |      from alefdw_stage.staging_practice_session st
        |               join alefdw.dim_learning_objective dlo on st.lo_uuid = dlo.lo_id
        |               join alefdw_stage.rel_user ru on st.student_uuid = ru.user_id
        |               left join alefdw.dim_subject dsu on st.subject_uuid = dsu.subject_id
        |               join alefdw.dim_grade dg on st.grade_uuid = dg.grade_id
        |               join alefdw.dim_tenant dt on st.tenant_uuid = dt.tenant_id
        |               join alefdw.dim_school ds on st.school_uuid = ds.school_id
        |               left join alefdw.dim_section dsec on st.section_uuid = dsec.section_id
        |               left join alefdw.dim_academic_year day on st.academic_year_uuid = day.academic_year_id
        |               left join alefdw.dim_class dcls on st.class_uuid = dcls.class_id and dcls.class_status = 1
        |      where st.practice_session_is_start = true
        |        and st.practice_session_is_start_event_processed = false
        |        and (
        |         ((st.subject_uuid isnull and dsu.subject_dw_id isnull) or (st.subject_uuid notnull and dsu.subject_dw_id notnull)) and
        |         ((st.section_uuid isnull and dsec.section_dw_id isnull) or (st.section_uuid notnull and dsec.section_dw_id notnull)) and
        |         ((st.class_uuid isnull and dcls.class_dw_id isnull) or (st.class_uuid notnull and dcls.class_dw_id notnull)) and
        |         ((st.academic_year_uuid isnull and day.academic_year_dw_id isnull) or (st.academic_year_uuid notnull and day.academic_year_dw_id notnull))
        |        )
        |        and st.practice_session_event_type = 1
        |      union all
        |      select st.practice_session_staging_id               as start_id,
        |             st.practice_session_created_time             as practice_session_start_time,
        |             cast(null as timestamp)                      as practice_session_end_time,
        |             getdate()                                    as practice_session_dw_created_time,
        |             st.practice_session_date_dw_id               as practice_session_date_dw_id,
        |             st.practice_session_id                       as practice_session_id,
        |             dlo.lo_dw_id                                 as practice_session_lo_dw_id,
        |             ru.user_dw_id                                as practice_session_student_dw_id,
        |             dsu.subject_dw_id                            as practice_session_subject_dw_id,
        |             dg.grade_dw_id                               as practice_session_grade_dw_id,
        |             dt.tenant_dw_id                              as practice_session_tenant_dw_id,
        |             ds.school_dw_id                              as practice_session_school_dw_id,
        |             dsec.section_dw_id                           as practice_session_section_dw_id,
        |             st.practice_session_sa_score                 as practice_session_sa_score,
        |             st.practice_session_item_content_uuid        as practice_session_item_content_uuid,
        |             st.practice_session_item_content_title       as practice_session_item_content_title,
        |             st.practice_session_item_content_lesson_type as practice_session_item_content_lesson_type,
        |             st.practice_session_item_content_location    as practice_session_item_content_location,
        |             st.practice_session_event_type               as practice_session_event_type,
        |             cast(null as int)                            as practice_session_time_spent,
        |             cast(null as bigint)                         as practice_session_score,
        |             st.practice_session_is_start                 as practice_session_is_start,
        |             st.practice_session_outside_of_school        as practice_session_outside_of_school,
        |             st.practice_session_stars                    as practice_session_stars,
        |             day.academic_year_dw_id                      as practice_session_academic_year_dw_id,
        |             st.practice_session_instructional_plan_id    as practice_session_instructional_plan_id,
        |             st.practice_session_learning_path_id         as practice_session_learning_path_id,
        |             dcls.class_dw_id                             as practice_session_class_dw_id,
        |             st.practice_session_material_id              as practice_session_material_id,
        |             st.practice_session_material_type            as practice_session_material_type,
        |             dlo1.lo_dw_id                                as practice_session_item_lo_dw_id,
        |             st.practice_session_item_step_id             as practice_session_item_step_id
        |      from alefdw_stage.staging_practice_session st
        |               join alefdw.dim_learning_objective dlo on st.lo_uuid = dlo.lo_id
        |               join alefdw_stage.rel_user ru on st.student_uuid = ru.user_id
        |               left join alefdw.dim_subject dsu on st.subject_uuid = dsu.subject_id
        |               join alefdw.dim_grade dg on st.grade_uuid = dg.grade_id
        |               join alefdw.dim_tenant dt on st.tenant_uuid = dt.tenant_id
        |               join alefdw.dim_school ds on st.school_uuid = ds.school_id
        |               left join alefdw.dim_section dsec on st.section_uuid = dsec.section_id
        |               left join alefdw.dim_academic_year day on st.academic_year_uuid = day.academic_year_id
        |               left join alefdw.dim_class dcls on st.class_uuid = dcls.class_id and dcls.class_status = 1
        |               join alefdw.dim_learning_objective dlo1 on st.practice_session_item_lo_uuid = dlo1.lo_id
        |      where st.practice_session_is_start = true
        |        and st.practice_session_is_start_event_processed = false
        |        and (
        |         ((st.subject_uuid isnull and dsu.subject_dw_id isnull) or (st.subject_uuid notnull and dsu.subject_dw_id notnull)) and
        |         ((st.section_uuid isnull and dsec.section_dw_id isnull) or (st.section_uuid notnull and dsec.section_dw_id notnull)) and
        |         ((st.class_uuid isnull and dcls.class_dw_id isnull) or (st.class_uuid notnull and dcls.class_dw_id notnull)) and
        |         ((st.academic_year_uuid isnull and day.academic_year_dw_id isnull) or (st.academic_year_uuid notnull and day.academic_year_dw_id notnull))
        |        )
        |        and st.practice_session_event_type IN (2, 3)) as a
        |order by a.start_id
        |limit 60000;
        |""".stripMargin)


    replaceSpecChars(sqlStringData.idSelectForDelete) shouldBe replaceSpecChars(
      """
        |select a.start_id, a.end_id
        |from (select st.practice_session_staging_id                                                       as start_id,
        |             en.practice_session_staging_id                                                       as end_id,
        |             st.practice_session_created_time                                                     as practice_session_start_time,
        |             en.practice_session_created_time                                                     as practice_session_end_time,
        |             getdate()                                                                            as practice_session_dw_created_time,
        |             en.practice_session_date_dw_id                                                       as practice_session_date_dw_id,
        |             st.practice_session_id                                                               as practice_session_id,
        |             dlo.lo_dw_id                                                                         as practice_session_lo_dw_id,
        |             ru.user_dw_id                                                                        as practice_session_student_dw_id,
        |             dsu.subject_dw_id                                                                    as practice_session_subject_dw_id,
        |             dg.grade_dw_id                                                                       as practice_session_grade_dw_id,
        |             dt.tenant_dw_id                                                                      as practice_session_tenant_dw_id,
        |             ds.school_dw_id                                                                      as practice_session_school_dw_id,
        |             dsec.section_dw_id                                                                   as practice_session_section_dw_id,
        |             st.practice_session_item_content_uuid                                                as practice_session_item_content_uuid,
        |             st.practice_session_item_content_title                                               as practice_session_item_content_title,
        |             st.practice_session_item_content_lesson_type                                         as practice_session_item_content_lesson_type,
        |             st.practice_session_item_content_location                                            as practice_session_item_content_location,
        |             st.practice_session_sa_score                                                         as practice_session_sa_score,
        |             en.practice_session_score                                                            as practice_session_score,
        |             st.practice_session_event_type                                                       as practice_session_event_type,
        |             DATEDIFF(second, st.practice_session_created_time,
        |                      en.practice_session_created_time)                                           as practice_session_time_spent,
        |             en.practice_session_is_start                                                         as practice_session_is_start,
        |             en.practice_session_outside_of_school                                                as practice_session_outside_of_school,
        |             en.practice_session_stars                                                            as practice_session_stars,
        |             day.academic_year_dw_id                                                              as practice_session_academic_year_dw_id,
        |             en.practice_session_instructional_plan_id                                            as practice_session_instructional_plan_id,
        |             en.practice_session_learning_path_id                                                 as practice_session_learning_path_id,
        |             dcls.class_dw_id                                                                     as practice_session_class_dw_id,
        |             en.practice_session_material_id                                                      as practice_session_material_id,
        |             en.practice_session_material_type                                                    as practice_session_material_type,
        |             null                                                                                 as practice_session_item_lo_dw_id,
        |             en.practice_session_item_step_id                                                     as practice_session_item_step_id
        |      from alefdw_stage.staging_practice_session st
        |               join alefdw_stage.staging_practice_session en on st.practice_session_id = en.practice_session_id and
        |                                                                st.practice_session_event_type =
        |                                                                en.practice_session_event_type and
        |                                                                st.practice_session_is_start = true and
        |                                                                en.practice_session_is_start = false
        |               join alefdw.dim_learning_objective dlo on st.lo_uuid = dlo.lo_id
        |               join alefdw_stage.rel_user ru on st.student_uuid = ru.user_id
        |               left join alefdw.dim_subject dsu on st.subject_uuid = dsu.subject_id
        |               join alefdw.dim_grade dg on st.grade_uuid = dg.grade_id
        |               join alefdw.dim_tenant dt on st.tenant_uuid = dt.tenant_id
        |               join alefdw.dim_school ds on st.school_uuid = ds.school_id
        |               left join alefdw.dim_section dsec on st.section_uuid = dsec.section_id
        |               left join alefdw.dim_academic_year day on st.academic_year_uuid = day.academic_year_id
        |               left join alefdw.dim_class dcls on st.class_uuid = dcls.class_id and dcls.class_status = 1
        |      where st.practice_session_event_type = 1
        |        and (
        |         ((st.subject_uuid isnull and dsu.subject_dw_id isnull) or (st.subject_uuid notnull and dsu.subject_dw_id notnull)) and
        |         ((st.section_uuid isnull and dsec.section_dw_id isnull) or (st.section_uuid notnull and dsec.section_dw_id notnull)) and
        |         ((st.class_uuid isnull and dcls.class_dw_id isnull) or (st.class_uuid notnull and dcls.class_dw_id notnull)) and
        |         ((st.academic_year_uuid isnull and day.academic_year_dw_id isnull) or (st.academic_year_uuid notnull and day.academic_year_dw_id notnull))
        |        )
        |      union all
        |      select st.practice_session_staging_id                                                       as start_id,
        |             en.practice_session_staging_id                                                       as end_id,
        |             st.practice_session_created_time                                                     as practice_session_start_time,
        |             en.practice_session_created_time                                                     as practice_session_end_time,
        |             getdate()                                                                            as practice_session_dw_created_time,
        |             en.practice_session_date_dw_id                                                       as practice_session_date_dw_id,
        |             st.practice_session_id                                                               as practice_session_id,
        |             dlo.lo_dw_id                                                                         as practice_session_lo_dw_id,
        |             ru.user_dw_id                                                                        as practice_session_student_dw_id,
        |             dsu.subject_dw_id                                                                    as practice_session_subject_dw_id,
        |             dg.grade_dw_id                                                                       as practice_session_grade_dw_id,
        |             dt.tenant_dw_id                                                                      as practice_session_tenant_dw_id,
        |             ds.school_dw_id                                                                      as practice_session_school_dw_id,
        |             dsec.section_dw_id                                                                   as practice_session_section_dw_id,
        |             st.practice_session_item_content_uuid                                                as practice_session_item_content_uuid,
        |             st.practice_session_item_content_title                                               as practice_session_item_content_title,
        |             st.practice_session_item_content_lesson_type                                         as practice_session_item_content_lesson_type,
        |             st.practice_session_item_content_location                                            as practice_session_item_content_location,
        |             st.practice_session_sa_score                                                         as practice_session_sa_score,
        |             en.practice_session_score                                                            as practice_session_score,
        |             st.practice_session_event_type                                                       as practice_session_event_type,
        |             DATEDIFF(second, st.practice_session_created_time,
        |                      en.practice_session_created_time)                                           as practice_session_time_spent,
        |             en.practice_session_is_start                                                         as practice_session_is_start,
        |             en.practice_session_outside_of_school                                                as practice_session_outside_of_school,
        |             en.practice_session_stars                                                            as practice_session_stars,
        |             day.academic_year_dw_id                                                              as practice_session_academic_year_dw_id,
        |             en.practice_session_instructional_plan_id                                            as practice_session_instructional_plan_id,
        |             en.practice_session_learning_path_id                                                 as practice_session_learning_path_id,
        |             dcls.class_dw_id                                                                     as practice_session_class_dw_id,
        |             en.practice_session_material_id                                                      as practice_session_material_id,
        |             en.practice_session_material_type                                                    as practice_session_material_type,
        |             dlo1.lo_dw_id                                                                        as practice_session_item_lo_dw_id,
        |             en.practice_session_item_step_id                                                     as practice_session_item_step_id
        |      from alefdw_stage.staging_practice_session st
        |               join alefdw_stage.staging_practice_session en on st.practice_session_id = en.practice_session_id and
        |                                                                st.practice_session_event_type =
        |                                                                en.practice_session_event_type and
        |                                                                st.practice_session_is_start = true and
        |                                                                en.practice_session_is_start = false and
        |                                                                st.practice_session_item_lo_uuid =
        |                                                                en.practice_session_item_lo_uuid
        |               join alefdw.dim_learning_objective dlo on st.lo_uuid = dlo.lo_id
        |               join alefdw_stage.rel_user ru on st.student_uuid = ru.user_id
        |               left join alefdw.dim_subject dsu on st.subject_uuid = dsu.subject_id
        |               join alefdw.dim_grade dg on st.grade_uuid = dg.grade_id
        |               join alefdw.dim_tenant dt on st.tenant_uuid = dt.tenant_id
        |               join alefdw.dim_school ds on st.school_uuid = ds.school_id
        |               left join alefdw.dim_section dsec on st.section_uuid = dsec.section_id
        |               left join alefdw.dim_academic_year day on st.academic_year_uuid = day.academic_year_id
        |               left join alefdw.dim_class dcls on st.class_uuid = dcls.class_id and dcls.class_status = 1
        |               join alefdw.dim_learning_objective dlo1 on st.practice_session_item_lo_uuid = dlo1.lo_id
        |      where st.practice_session_event_type = 2
        |        and (
        |         ((st.subject_uuid isnull and dsu.subject_dw_id isnull) or (st.subject_uuid notnull and dsu.subject_dw_id notnull)) and
        |         ((st.section_uuid isnull and dsec.section_dw_id isnull) or (st.section_uuid notnull and dsec.section_dw_id notnull)) and
        |         ((st.class_uuid isnull and dcls.class_dw_id isnull) or (st.class_uuid notnull and dcls.class_dw_id notnull)) and
        |         ((st.academic_year_uuid isnull and day.academic_year_dw_id isnull) or (st.academic_year_uuid notnull and day.academic_year_dw_id notnull))
        |        )
        |      union all
        |      select st.practice_session_staging_id                                                       as start_id,
        |             en.practice_session_staging_id                                                       as end_id,
        |             st.practice_session_created_time                                                     as practice_session_start_time,
        |             en.practice_session_created_time                                                     as practice_session_end_time,
        |             getdate()                                                                            as practice_session_dw_created_time,
        |             en.practice_session_date_dw_id                                                       as practice_session_date_dw_id,
        |             st.practice_session_id                                                               as practice_session_id,
        |             dlo.lo_dw_id                                                                         as practice_session_lo_dw_id,
        |             ru.user_dw_id                                                                        as practice_session_student_dw_id,
        |             dsu.subject_dw_id                                                                    as practice_session_subject_dw_id,
        |             dg.grade_dw_id                                                                       as practice_session_grade_dw_id,
        |             dt.tenant_dw_id                                                                      as practice_session_tenant_dw_id,
        |             ds.school_dw_id                                                                      as practice_session_school_dw_id,
        |             dsec.section_dw_id                                                                   as practice_session_section_dw_id,
        |             st.practice_session_item_content_uuid                                                as practice_session_item_content_uuid,
        |             st.practice_session_item_content_title                                               as practice_session_item_content_title,
        |             st.practice_session_item_content_lesson_type                                         as practice_session_item_content_lesson_type,
        |             st.practice_session_item_content_location                                            as practice_session_item_content_location,
        |             st.practice_session_sa_score                                                         as practice_session_sa_score,
        |             en.practice_session_score                                                            as practice_session_score,
        |             st.practice_session_event_type                                                       as practice_session_event_type,
        |             DATEDIFF(second, st.practice_session_created_time,
        |                      en.practice_session_created_time)                                           as practice_session_time_spent,
        |             en.practice_session_is_start                                                         as practice_session_is_start,
        |             en.practice_session_outside_of_school                                                as practice_session_outside_of_school,
        |             en.practice_session_stars                                                            as practice_session_stars,
        |             day.academic_year_dw_id                                                              as practice_session_academic_year_dw_id,
        |             en.practice_session_instructional_plan_id                                            as practice_session_instructional_plan_id,
        |             en.practice_session_learning_path_id                                                 as practice_session_learning_path_id,
        |             dcls.class_dw_id                                                                     as practice_session_class_dw_id,
        |             en.practice_session_material_id                                                      as practice_session_material_id,
        |             en.practice_session_material_type                                                    as practice_session_material_type,
        |             dlo1.lo_dw_id                                                                        as practice_session_item_lo_dw_id,
        |             en.practice_session_item_step_id                                                     as practice_session_item_step_id
        |      from alefdw_stage.staging_practice_session st
        |               join alefdw_stage.staging_practice_session en on st.practice_session_id = en.practice_session_id and
        |                                                                st.practice_session_event_type =
        |                                                                en.practice_session_event_type and
        |                                                                st.practice_session_is_start = true and
        |                                                                en.practice_session_is_start = false and
        |                                                                st.practice_session_item_lo_uuid =
        |                                                                en.practice_session_item_lo_uuid and
        |                                                                st.practice_session_item_step_id =
        |                                                                en.practice_session_item_step_id
        |               join alefdw.dim_learning_objective dlo on st.lo_uuid = dlo.lo_id
        |               join alefdw_stage.rel_user ru on st.student_uuid = ru.user_id
        |               left join alefdw.dim_subject dsu on st.subject_uuid = dsu.subject_id
        |               join alefdw.dim_grade dg on st.grade_uuid = dg.grade_id
        |               join alefdw.dim_tenant dt on st.tenant_uuid = dt.tenant_id
        |               join alefdw.dim_school ds on st.school_uuid = ds.school_id
        |               left join alefdw.dim_section dsec on st.section_uuid = dsec.section_id
        |               left join alefdw.dim_academic_year day on st.academic_year_uuid = day.academic_year_id
        |               left join alefdw.dim_class dcls on st.class_uuid = dcls.class_id and dcls.class_status = 1
        |               join alefdw.dim_learning_objective dlo1 on st.practice_session_item_lo_uuid = dlo1.lo_id
        |      where st.practice_session_event_type = 3
        |        and (
        |         ((st.subject_uuid isnull and dsu.subject_dw_id isnull) or (st.subject_uuid notnull and dsu.subject_dw_id notnull)) and
        |         ((st.section_uuid isnull and dsec.section_dw_id isnull) or (st.section_uuid notnull and dsec.section_dw_id notnull)) and
        |         ((st.class_uuid isnull and dcls.class_dw_id isnull) or (st.class_uuid notnull and dcls.class_dw_id notnull)) and
        |         ((st.academic_year_uuid isnull and day.academic_year_dw_id isnull) or (st.academic_year_uuid notnull and day.academic_year_dw_id notnull))
        |        )) as a
        |order by a.start_id
        |limit 60000;
        |""".stripMargin)
  }
}
