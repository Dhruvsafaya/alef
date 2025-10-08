package com.alefeducation.schema.ccl

case class CurriculumCreatedEvent(curriculumId: Long, name: String, organisation:String, occurredOn: Long)

case class CurriculumGradeCreatedEvent(gradeId: Long, name: String, occurredOn: Long)

case class CurriculumSubjectCreatedEvent(subjectId: Long, name: String, skillable: Boolean, occurredOn: Long)

case class CurriculumSubjectUpdatedEvent(subjectId: Long, name: String, skillable: Boolean, occurredOn: Long)

case class CurriculumSubjectDeletedEvent(subjectId: Long, occurredOn: Long)

case class CurriculumGradeLinkedEvent(curriculumId: Long, gradeId: Long, occurredOn: Long)

case class CurriculumGradeSubjectLinkedEvent(curriculumId: Long, gradeId: Long, subjectId: Long, occurredOn: Long)

case class CurriculumGradeSubjectUnlinkedEvent(curriculumId: Long, gradeId: Long, subjectId: Long, occurredOn: Long)
