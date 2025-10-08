package com.alefeducation.schema.notification

case class AwardResource(id: String,
                         schoolId: String,
                         grade: Int,
                         gradeId: String,
                         section: String,
                         trimesterId: String,
                         classId: String,
                         subjectId: String,
                         learnerId: String,
                         teacherId: String,
                         categoryCode: String,
                         categoryLabelEn: String,
                         categoryLabelAr: String,
                         comment: String,
                         createdOn: String,
                         academicYearId: String,
                         stars: Int)
