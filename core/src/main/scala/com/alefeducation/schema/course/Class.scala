package com.alefeducation.schema.course

case class ClassMutated(
    classId: String,
    title: String,
    schoolId: String,
    gradeId: String,
    sectionId: String,
    academicYearId: String,
    subjectCategory: String,
    material: ClassMaterial,
    settings: ClassSettings,
    teachers: Set[String],
    status: String,
    sourceId: String,
    occurredOn: Long,
    categoryId: String,
    organisationGlobal: String,
    academicCalendarId: String
 )

case class ClassDeleted(classId: String, occurredOn: Long)

case class ClassMaterial(curriculum: String,
                         grade: String,
                         subject: String,
                         year: Int,
                         instructionalPlanId: String,
                         materialId: String,
                         materialType: String
                        )

case class ClassSettings(tutorDhabiEnabled: Boolean, languageDirection: String, online: Boolean, practice: Boolean, studentProgressView: String)

case class StudentClassEnrollmentToggle(classId: String, userId: String, occurredOn: Long)

case class ClassScheduleSlot(classId: String, day: String, startTime: String, endTime: String, occurredOn: Long)

case class ClassCategory(classCategoryId: String, name: String, occurredOn: Long, organisationGlobal: String)
