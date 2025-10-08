package com.alefeducation.schema.lps

case class LearningObjective(id: String, title: String, code: String, `type`: String, createdOn: Long,
                             curriculumId: String, curriculumSubjectId: String, curriculumGradeId: String, academicYear: String,
                             order: Long)

case class LearningObjectiveUpdatedDeleted(id: String, title: String, code: String, `type`: String, occurredOn: Long,
                             curriculumId: String, curriculumSubjectId: String, curriculumGradeId: String, academicYear: String,
                             order: Long)