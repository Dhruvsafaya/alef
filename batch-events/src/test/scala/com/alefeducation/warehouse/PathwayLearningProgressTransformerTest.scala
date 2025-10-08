package com.alefeducation.warehouse

import com.alefeducation.warehouse.models.WarehouseConnection
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scalikejdbc.AutoSession

class PathwayLearningProgressTransformerTest extends AnyFunSuite with Matchers {
  test("should prepare queries") {
    val connection = WarehouseConnection("testalefdw", "http://localhost:8080", "jdbc", "username", "password")
    implicit val autoSession: AutoSession = AutoSession
    val queryMetas = PathwayLearningProgressTransformer.prepareQueries(connection)

    queryMetas.head.stagingTable should be("staging_pathway_learning_progress")
    queryMetas.head.selectSQL should be(expectedSelectStatement)
    queryMetas.head.insertSQL.stripMargin.trim should be(expectedInsertStatement.stripMargin.trim)
  }

  private def expectedSelectStatement: String =
    """ select dw_id
      |from testalefdw_stage.staging_pathway_learning_progress
      | left join (select lo_id, lo_dw_id from (
      |         select lo_id, lo_dw_id, row_number() over (partition by lo_id order by lo_created_time desc) as rank
      |         from testalefdw.dim_learning_objective
      |          ) t where t.rank = 1
      |       ) AS dlo ON dlo.lo_id = staging_pathway_learning_progress.learning_objective_id
      | left join testalefdw.dim_interim_checkpoint
      |    on dim_interim_checkpoint.ic_id = staging_pathway_learning_progress.learning_objective_id
      |inner join testalefdw_stage.rel_user student1
      |    on student1.user_id = staging_pathway_learning_progress.student_id
      |left join testalefdw.dim_subject
      |    on dim_subject.subject_id = staging_pathway_learning_progress.subject_id
      |inner join testalefdw.dim_grade
      |    on dim_grade.grade_id = staging_pathway_learning_progress.student_grade_id
      |left join testalefdw.dim_curriculum_subject
      |    on dim_curriculum_subject.curr_subject_id = staging_pathway_learning_progress.curriculum_subject_id and
      |                                    dim_curriculum_subject.curr_subject_status = 1
      |left join testalefdw.dim_curriculum_grade
      |    on dim_curriculum_grade.curr_grade_id = staging_pathway_learning_progress.curriculum_grade_id
      |inner join testalefdw.dim_tenant
      |    on dim_tenant.tenant_id = staging_pathway_learning_progress.tenant_id
      |inner join testalefdw.dim_school
      |    on dim_school.school_id = staging_pathway_learning_progress.school_id
      |left join testalefdw.dim_section
      |    on dim_section.section_id = staging_pathway_learning_progress.student_section
      |left join testalefdw.dim_academic_year
      |    on dim_academic_year.academic_year_id = staging_pathway_learning_progress.academic_year_id
      |left join (select class_dw_id, class_id
      |                    from (select
      |                              class_dw_id,
      |                              class_id,
      |                              row_number() over (partition by class_id order by class_created_time desc) as rank
      |                              from testalefdw.dim_class
      |                          ) where rank = 1
      |                    ) as dc
      |                   on dc.class_id = staging_pathway_learning_progress.class_id
      |
      |left join (select instructional_plan_id as ip_instructional_plan_id,
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
      |                     from testalefdw.dim_instructional_plan
      |                    ) where rank = 1) as ip on
      |                        ip.ip_instructional_plan_id = staging_pathway_learning_progress.material_id and
      |                        staging_pathway_learning_progress.material_type = 'INSTRUCTIONAL_PLAN' and
      |                        (dlo.lo_dw_id is not null and
      |                         ip.instructional_plan_item_lo_dw_id = dlo.lo_dw_id and
      |                         nvl(ip.instructional_plan_item_type, '') != 'TEST' or
      |                         dim_interim_checkpoint.ic_dw_id is not null and
      |                         staging_pathway_learning_progress.activity_type = 'INTERIM_CHECKPOINT' and
      |                         ip.instructional_plan_item_ic_dw_id = dim_interim_checkpoint.ic_dw_id)
      |
      |left join testalefdw.dim_week
      |      on ip.instructional_plan_item_week_dw_id = dim_week.week_dw_id
      |left join testalefdw.dim_term
      |      on dim_term.term_id = dim_week.week_term_id
      |where
      | ((staging_pathway_learning_progress.subject_id isnull and dim_subject.subject_dw_id isnull) or (staging_pathway_learning_progress.subject_id notnull and dim_subject.subject_dw_id notnull))  and
      | ((student_section isnull and dim_section.section_dw_id isnull) or (student_section notnull and dim_section.section_dw_id notnull))  and
      | ((staging_pathway_learning_progress.class_id isnull and dc.class_dw_id isnull) or (staging_pathway_learning_progress.class_id notnull and dc.class_dw_id notnull)) and
      | (dlo.lo_id is not null or dim_interim_checkpoint.ic_id is not null) and
      | (dim_term.term_dw_id is not null and staging_pathway_learning_progress.material_type = 'INSTRUCTIONAL_PLAN' or dim_term.term_dw_id is null and staging_pathway_learning_progress.material_type IN ('PATHWAY')) AND
      | ((staging_pathway_learning_progress.academic_year_id IS NULL AND dim_academic_year.academic_year_dw_id IS NULL) OR (staging_pathway_learning_progress.academic_year_id IS NOT NULL AND dim_academic_year.academic_year_dw_id IS NOT NULL))
      |
      |order by dw_id
      |limit 60000
      |""".stripMargin

  private def expectedInsertStatement: String =
    """
      |INSERT INTO testalefdw.fact_pathway_learning_progress (dw_id,
      |	created_time,
      |	dw_created_time,
      |	date_dw_id,
      |	experience_id,
      |	_trace_id,
      |	event_type,
      |	learning_session_id,
      |	start_time,
      |	end_time,
      |	total_time,
      |	score,
      |	stars,
      |	lesson_type,
      |	retry,
      |	outside_of_school,
      |	attempt,
      |	learning_experience_flag,
      |	trimester_order,
      |	content_academic_year,
      |	time_spent,
      |	instructional_plan_id,
      |	lesson_category,
      |	level,
      |	content_id,
      |	abbreviation,
      |	activity_template_id,
      |	activity_type,
      |	activity_component_type,
      |	exit_ticket,
      |	main_component,
      |	completion_node,
      |	total_score,
      |	activity_completed,
      |	material_id,
      |	material_type,
      |	state,
      |	open_path_enabled,
      |	source,
      |	teaching_period_id,
      |	academic_year,
      |	bonus_stars,
      |	bonus_stars_scheme,
      |	learning_objective_dw_id,
      |	student_dw_id,
      |	subject_dw_id,
      |	student_grade_dw_id,
      |	class_dw_id,
      |	curriculum_subject_dw_id,
      |	curriculum_grade_dw_id,
      |	term_dw_id,
      |	tenant_dw_id,
      |	school_dw_id,
      |	student_section_dw_id,
      |	academic_year_dw_id)
      | ( select dw_id,
      |	created_time,
      |	getdate(),
      |	date_dw_id,
      |	experience_id,
      |	_trace_id,
      |	event_type,
      |	learning_session_id,
      |	start_time,
      |	end_time,
      |	total_time,
      |	score,
      |	stars,
      |	lesson_type,
      |	retry,
      |	outside_of_school,
      |	attempt,
      |	learning_experience_flag,
      |	trimester_order,
      |	content_academic_year,
      |	time_spent,
      |	instructional_plan_id,
      |	lesson_category,
      |	level,
      |	content_id,
      |	abbreviation,
      |	activity_template_id,
      |	activity_type,
      |	activity_component_type,
      |	exit_ticket,
      |	main_component,
      |	completion_node,
      |	total_score,
      |	activity_completed,
      |	material_id,
      |	material_type,
      |	state,
      |	open_path_enabled,
      |	source,
      |	teaching_period_id,
      |	academic_year,
      |	bonus_stars,
      |	bonus_stars_scheme,
      |	CASE WHEN staging_pathway_learning_progress.activity_type = 'INTERIM_CHECKPOINT' THEN dim_interim_checkpoint.ic_dw_id ELSE dlo.lo_dw_id END AS learning_objective_dw_id,
      |	student1.user_dw_id,
      |	dim_subject.subject_dw_id,
      |	dim_grade.grade_dw_id,
      |	dc.class_dw_id,
      |	dim_curriculum_subject.curr_subject_dw_id,
      |	dim_curriculum_grade.curr_grade_dw_id,
      |	dim_term.term_dw_id,
      |	dim_tenant.tenant_dw_id,
      |	dim_school.school_dw_id,
      |	dim_section.section_dw_id,
      |	dim_academic_year.academic_year_dw_id
      |from testalefdw_stage.staging_pathway_learning_progress
      | left join (select lo_id, lo_dw_id from (
      |         select lo_id, lo_dw_id, row_number() over (partition by lo_id order by lo_created_time desc) as rank
      |         from testalefdw.dim_learning_objective
      |          ) t where t.rank = 1
      |       ) AS dlo ON dlo.lo_id = staging_pathway_learning_progress.learning_objective_id
      | left join testalefdw.dim_interim_checkpoint
      |    on dim_interim_checkpoint.ic_id = staging_pathway_learning_progress.learning_objective_id
      |inner join testalefdw_stage.rel_user student1
      |    on student1.user_id = staging_pathway_learning_progress.student_id
      |left join testalefdw.dim_subject
      |    on dim_subject.subject_id = staging_pathway_learning_progress.subject_id
      |inner join testalefdw.dim_grade
      |    on dim_grade.grade_id = staging_pathway_learning_progress.student_grade_id
      |left join testalefdw.dim_curriculum_subject
      |    on dim_curriculum_subject.curr_subject_id = staging_pathway_learning_progress.curriculum_subject_id and
      |                                    dim_curriculum_subject.curr_subject_status = 1
      |left join testalefdw.dim_curriculum_grade
      |    on dim_curriculum_grade.curr_grade_id = staging_pathway_learning_progress.curriculum_grade_id
      |inner join testalefdw.dim_tenant
      |    on dim_tenant.tenant_id = staging_pathway_learning_progress.tenant_id
      |inner join testalefdw.dim_school
      |    on dim_school.school_id = staging_pathway_learning_progress.school_id
      |left join testalefdw.dim_section
      |    on dim_section.section_id = staging_pathway_learning_progress.student_section
      |left join testalefdw.dim_academic_year
      |    on dim_academic_year.academic_year_id = staging_pathway_learning_progress.academic_year_id
      |left join (select class_dw_id, class_id
      |                    from (select
      |                              class_dw_id,
      |                              class_id,
      |                              row_number() over (partition by class_id order by class_created_time desc) as rank
      |                              from testalefdw.dim_class
      |                          ) where rank = 1
      |                    ) as dc
      |                   on dc.class_id = staging_pathway_learning_progress.class_id
      |
      |left join (select instructional_plan_id as ip_instructional_plan_id,
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
      |                     from testalefdw.dim_instructional_plan
      |                    ) where rank = 1) as ip on
      |                        ip.ip_instructional_plan_id = staging_pathway_learning_progress.material_id and
      |                        staging_pathway_learning_progress.material_type = 'INSTRUCTIONAL_PLAN' and
      |                        (dlo.lo_dw_id is not null and
      |                         ip.instructional_plan_item_lo_dw_id = dlo.lo_dw_id and
      |                         nvl(ip.instructional_plan_item_type, '') != 'TEST' or
      |                         dim_interim_checkpoint.ic_dw_id is not null and
      |                         staging_pathway_learning_progress.activity_type = 'INTERIM_CHECKPOINT' and
      |                         ip.instructional_plan_item_ic_dw_id = dim_interim_checkpoint.ic_dw_id)
      |
      |left join testalefdw.dim_week
      |      on ip.instructional_plan_item_week_dw_id = dim_week.week_dw_id
      |left join testalefdw.dim_term
      |      on dim_term.term_id = dim_week.week_term_id
      |where
      | ((staging_pathway_learning_progress.subject_id isnull and dim_subject.subject_dw_id isnull) or (staging_pathway_learning_progress.subject_id notnull and dim_subject.subject_dw_id notnull))  and
      | ((student_section isnull and dim_section.section_dw_id isnull) or (student_section notnull and dim_section.section_dw_id notnull))  and
      | ((staging_pathway_learning_progress.class_id isnull and dc.class_dw_id isnull) or (staging_pathway_learning_progress.class_id notnull and dc.class_dw_id notnull)) and
      | (dlo.lo_id is not null or dim_interim_checkpoint.ic_id is not null) and
      | (dim_term.term_dw_id is not null and staging_pathway_learning_progress.material_type = 'INSTRUCTIONAL_PLAN' or dim_term.term_dw_id is null and staging_pathway_learning_progress.material_type IN ('PATHWAY')) AND
      | ((staging_pathway_learning_progress.academic_year_id IS NULL AND dim_academic_year.academic_year_dw_id IS NULL) OR (staging_pathway_learning_progress.academic_year_id IS NOT NULL AND dim_academic_year.academic_year_dw_id IS NOT NULL))
      |
      |order by dw_id
      |limit 60000
      |
      |);""".stripMargin

}
