package com.alefeducation.models

import java.sql.Timestamp

object ClassModel {
  case class DimClass(class_id: String,
                      class_title: String,
                      class_school_id: String,
                      class_grade_id: String,
                      class_section_id: String,
                      class_academic_year_id: String,
                      class_academic_calendar_id: String,
                      class_gen_subject: String,
                      class_curriculum_id: Long,
                      class_curriculum_grade_id: Long,
                      class_curriculum_subject_id: Long,
                      class_content_academic_year: Int,
                      class_tutor_dhabi_enabled: Boolean,
                      class_language_direction: String,
                      class_online: Boolean,
                      class_practice: Boolean,
                      class_course_status: String,
                      class_source_id: String,
                      class_curriculum_instructional_plan_id: String,
                      class_category_id: String,
                      class_material_id: String,
                      class_material_type: String,
                      class_created_time: Timestamp,
                      class_status: Int)

}
