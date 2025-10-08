package com.alefeducation.models
import java.sql.Timestamp

object TeacherModel {

  case class TeacherMovedBetweenSchools(occurredOn: Long, teacherId: String, sourceSchoolId: String, targetSchoolId: String)
  case class TeacherMutated(occurredOn: String, uuid: String, schoolId: String, eventType: String, teacher_status: Int, loadtime: String)

  case class TeacherRel(rel_teacher_id: String,
                        teacher_created_time: Timestamp,
                        teacher_updated_time: Timestamp,
                        teacher_deleted_time: Timestamp,
                        teacher_dw_created_time: Timestamp,
                        teacher_dw_updated_time: Timestamp,
                        teacher_active_until: Timestamp,
                        teacher_status: Int,
                        teacher_id: String,
                        subject_id: String,
                        school_id: String)

  case class TeacherDim(rel_teacher_dw_id: Long,
                        teacher_created_time: Timestamp,
                        teacher_updated_time: Timestamp,
                        teacher_deleted_time: Timestamp,
                        teacher_dw_created_time: Timestamp,
                        teacher_dw_updated_time: Timestamp,
                        teacher_active_until: Timestamp,
                        teacher_status: Int,
                        teacher_id: String,
                        teacher_dw_id: Long,
                        teacher_school_dw_id: Long)

  case class SchoolDim(school_dw_id: Long,
                       school_created_time: Timestamp,
                       school_updated_time: Timestamp,
                       school_deleted_time: Timestamp,
                       school_dw_created_time: Timestamp,
                       school_dw_updated_time: Timestamp,
                       school_status: Int,
                       school_id: String,
                       school_name: String,
                       school_address_line: String,
                       school_post_box: String,
                       school_city_name: String,
                       school_country_name: String,
                       school_latitude: Double,
                       school_longitude: Double,
                       school_first_day: String,
                       school_timezone: String,
                       school_composition: String,
                       school_tenant_id: String,
                       school_alias: String)
}
