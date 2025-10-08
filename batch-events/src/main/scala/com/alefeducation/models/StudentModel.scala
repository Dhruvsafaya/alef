package com.alefeducation.models

import java.sql.Timestamp

object StudentModel {

  case class StudentStatus(occurredOn: String, uuid: String, student_status: Int, eventType: String)

  case class StudentMutated(occurredOn: String,
                            uuid: String,
                            username: String,
                            gradeId: String,
                            sectionId: String,
                            schoolId: String,
                            student_status: Int,
                            tags: String,
                            specialNeeds: String)

  case class StudentSectionUpdated(occurredOn: String,
                                   uuid: String,
                                   grade: String,
                                   gradeId: String,
                                   schoolId: String,
                                   oldSectionId: String,
                                   newSectionId: String)

  case class StudentTag(occurredOn: Long, studentId: String, tags: List[String])

  case class StudentDim(rel_student_dw_id: Long,
                        student_created_time: Timestamp,
                        student_updated_time: Timestamp,
                        student_deleted_time: Timestamp,
                        student_dw_created_time: Timestamp,
                        student_dw_updated_time: Timestamp,
                        student_active_until: Timestamp,
                        student_status: Int,
                        student_id: String,
                        student_username: String,
                        student_dw_id: Long,
                        student_school_dw_id: Long,
                        student_grade_dw_id: Long,
                        student_section_dw_id: Long,
                        student_tags: String,
                        student_special_needs: String)

  case class StudentRel(rel_student_id: Long,
                        student_created_time: Timestamp,
                        student_updated_time: Timestamp,
                        student_deleted_time: Timestamp,
                        student_dw_created_time: Timestamp,
                        student_dw_updated_time: Timestamp,
                        student_active_until: Timestamp,
                        student_status: Int,
                        student_uuid: String,
                        student_username: String,
                        school_uuid: String,
                        grade_uuid: String,
                        section_uuid: String,
                        student_tags: String,
                        student_special_needs: String)

  case class GradeDim(grade_dw_id: Long,
                      grade_created_time: Timestamp,
                      grade_updated_time: Timestamp,
                      grade_deleted_time: Timestamp,
                      grade_dw_created_time: Timestamp,
                      grade_dw_updated_time: Timestamp,
                      grade_status: Int,
                      grade_id: String,
                      grade_name: String,
                      grade_k12grade: String,
                      academic_year_id: String,
                      tenant_id: String,
                      school_id: String)

  case class SectionDim(section_dw_id: Long,
                        section_created_time: Timestamp,
                        section_updated_time: Timestamp,
                        section_deleted_time: Timestamp,
                        section_dw_created_time: Timestamp,
                        section_dw_updated_time: Timestamp,
                        section_status: Int,
                        section_id: String,
                        section_alias: String,
                        section_name: String,
                        section_enabled: Boolean,
                        tenant_id: String,
                        grade_id: String,
                        school_id: String)

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
                       school_alias: String,
                       school_content_repository_dw_id: Int,
                       school_source_id: String)

  lazy val ColsForSchoolFromDimRel = Map(
    "occurredOn" -> "occurredOnSchoolMovement",
    "uuid" -> "schoolStudentUuid",
    "schoolId" -> "schoolId"
  )
  lazy val ColsForSchoolFromMoveSchool = Map(
    "occurredOn" -> "occurredOnSchoolMovement",
    "studentId" -> "schoolStudentUuid",
    "targetSchoolId" -> "schoolId"
  )
  lazy val ColsForGradeSectionFromFromDimRel = Map(
    "occurredOn" -> "occurredOnGradeSectionMovement",
    "uuid" -> "gradeSectionStudentUuid",
    "gradeId" -> "gradeId",
    "sectionId" -> "sectionId"
  )
  lazy val ColsForGradeSectionFromMoveSchool = Map(
    "occurredOn" -> "occurredOnGradeSectionMovement",
    "studentId" -> "gradeSectionStudentUuid",
    "targetGradeId" -> "gradeId",
    "targetSectionId" -> "sectionId"
  )
  lazy val ColsForGradeSectionFromSectionUpdated = Map(
    "occurredOn" -> "occurredOnGradeSectionMovement",
    "uuid" -> "gradeSectionStudentUuid",
    "gradeId" -> "gradeId",
    "newSectionId" -> "sectionId"
  )
  lazy val ColsForGradeSectionFromPromoted = Map(
    "occurredOn" -> "occurredOnGradeSectionMovement",
    "studentId" -> "gradeSectionStudentUuid",
    "rolledOverGradeId" -> "gradeId",
    "rolledOverSectionId" -> "sectionId"
  )
  lazy val ColsForStatus = Map(
    "occurredOn" -> "occurredOnStatus",
    "uuid" -> "statusStudentUuid",
    "student_status" -> "student_status"
  )
  lazy val ColsForOtherFields = Map(
    "occurredOn" -> "occurredOnOtherFields",
    "uuid" -> "uuid",
    "username" -> "username",
    "tags" -> "tags",
    "specialNeeds" -> "specialNeeds"
  )
}
