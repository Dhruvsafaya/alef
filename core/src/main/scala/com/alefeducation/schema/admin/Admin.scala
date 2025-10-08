package com.alefeducation.schema.admin

import org.apache.spark.sql.types.{BooleanType, IntegerType, LongType, StringType, StructField, StructType}

case class SectionMutatedEvent(createdOn: Long,
                               uuid: String,
                               enabled: Boolean,
                               schoolId: String,
                               gradeId: String,
                               gradeName: String,
                               name: String,
                               section: String,
                               sourceId: String)

case class SectionStateChanged(createdOn: Long, uuid: String)

case class StudentSectionUpdatedEvent(occurredOn: Long,
                                      id: String,
                                      grade: String,
                                      gradeId: String,
                                      schoolId: String,
                                      oldSectionId: String,
                                      newSectionId: String)

case class School(createdOn: Long,
                  uuid: String,
                  addressId: String,
                  addressLine: String,
                  addressPostBox: String,
                  addressCity: String,
                  addressCountry: String,
                  latitude: String,
                  longitude: String,
                  organisation: String,
                  timeZone: String,
                  firstTeachingDay: String,
                  name: String,
                  composition: String,
                  alias: String,
                  sourceId: String,
                  contentRepositoryId: String,
                  organisationGlobal: String,
                  currentAcademicYearId: String,
                  contentRepositoryIds: List[String])

case class StudentMutatedEvent(createdOn: Long,
                               uuid: String,
                               username: String,
                               grade: String,
                               gradeId: String,
                               sectionId: String,
                               schoolId: String,
                               tags: Set[String],
                               specialNeeds: Set[String])

case class StudentDeleted(studentId: String, gradeName: Int, sectionId: String, gradeId: String, userName: String)

case class StudentsDeletedEvent(createdOn: Long, schoolId: String, events: List[StudentDeleted])

case class StudentMovedBetweenSchoolsOrGrades(occurredOn: Long,
                                              studentId: String,
                                              oldSchoolId: String,
                                              oldSectionId: String,
                                              oldGradeId: String,
                                              oldK12Grade: Int,
                                              targetSchoolId: String,
                                              targetSectionId: String,
                                              targetGradeId: String,
                                              targetK12Grade: Int)

case class StudentPromotedEvent(occurredOn: Long, studentId: String, rolledOverSectionId: String, rolledOverGradeId: String)

case class StudentToggleStatusEvent(createdOn: Long,
                                    uuid: String,
                                    username: String,
                                    k12Grade: Int,
                                    gradeId: String,
                                    sectionId: String,
                                    schoolId: String)

case class Grade(createdOn: Long, uuid: String, name: String, schoolId: String, k12Grade: Int, academicYearId: String)

case class GradeDeletedEvent(createdOn: Long, uuid: String)

case class Subject(createdOn: Long,
                   uuid: String,
                   name: String,
                   online: Boolean,
                   languageType: String,
                   experimentalContent: Boolean,
                   schoolId: String,
                   schoolGradeUuid: String,
                   schoolGradeName: String,
                   curriculumId: String,
                   curriculumName: String,
                   curriculumGradeId: String,
                   curriculumGradeName: String,
                   curriculumSubjectId: String,
                   curriculumSubjectName: String,
                   genSubject: String)

case class Teacher(createdOn: Long, uuid: String, enabled: Boolean, schoolId: String)

case class GuardianInvitedRegistered(occurredOn: Long, uuid: String)

case class GuardiansDeletedEvent(events: List[GuardianDeleted], occurredOn: Long)

case class GuardianDeleted(occurredOn: Long, uuid: String)

case class GuardianAssociation(occurredOn: Long, studentId: String, guardians: List[AssociatedGuardian])

case class AssociatedGuardian(id: String)

object LearningPath {
  val learningPathSchema: StructType = StructType(
    Seq(
      StructField("createdOn", LongType),
      StructField("id", StringType),
      StructField("name", StringType),
      StructField("status", StringType),
      StructField("languageTypeScript", StringType),
      StructField("experientialLearning", BooleanType),
      StructField("tutorDhabiEnabled", BooleanType),
      StructField("default", BooleanType),
      StructField("curriculumId", StringType),
      StructField("curriculumGradeId", StringType),
      StructField("curriculumSubjectId", StringType),
      StructField("schoolId", StringType),
      StructField("academicYearId", StringType),
      StructField("academicYear", IntegerType),
      StructField("grade", IntegerType),
      StructField("schoolSubjectId", StringType),
      StructField("classId", StringType)
    )
  )
}

case class TeacherMovedBetweenSchools(occurredOn: Long, teacherId: String, sourceSchoolId: String, targetSchoolId: String)

case class AcademicYearMutatedEvent(occurredOn: Long, id: String, endDate: Long, startDate: Long, schoolId: String)

case class AcademicYearRollOverEvent(occurredOn: Long, id: String, schoolId: String, previousId: String)

case class SchoolAcademicYearSwitched(occurredOn: Long,
                                      currentAcademicYearId: String,
                                      currentAcademicYearType: String,
                                      currentAcademicYearStartDate: Long,
                                      currentAcademicYearEndDate: Long,
                                      currentAcademicYearStatus: String,
                                      oldAcademicYearId: String,
                                      oldAcademicYearType: String,
                                      oldAcademicYearStartDate: Long,
                                      oldAcademicYearEndDate: Long,
                                      organization: String,
                                      schoolId: String,
                                      updatedBy: String)

case class Slot(dayOfWeek: String, startTime: SlotTime, endTime: SlotTime, teacherIds: List[String], subjectId: String)

case class SlotTime(hour: Int, minute: Int, second: Int)

case class StudentTagUpdatedEvent(occurredOn: Long, studentId: String, tags: Set[String])

case class TagEvent(id: String, tagId: String, name: String, `type`: String, occurredOn: Long)

case class SchoolStatusToggleEvent(schoolId: String, occurredOn: Long)

case class ContentRepositoryRedshift(content_repository_dw_id: Int, content_repository_id: String)

case class DwIdMapping(id: String, entity_type: String, dw_id: Int)

case class OrganizationRedshift(organization_dw_id: Int, organization_code: String)
