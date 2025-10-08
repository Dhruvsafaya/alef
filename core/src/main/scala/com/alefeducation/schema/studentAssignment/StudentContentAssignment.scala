package com.alefeducation.schema.studentAssignment

case class StudentContentAssignment(
    classId: String,
    studentId: String,
    contentId: String,
    contentType: String,
    assignedBy: String,
    mloId: String,
    occurredOn: Long
)

case class ClassContentAssignedUnAssignedEvent(
                                                classId: String,
                                                contentId: String,
                                                contentType: String,
                                                assignedBy: String,
                                                mloId: String,
                                                occurredOn: Long
                                              )

case class StudentLessonAssignment(
    classId: String,
    studentId: String,
    contentType: String,
    assignedBy: String,
    mloId: String,
    occurredOn: Long
)

case class ClassLessonAssignedUnAssignedEvent(
                                                classId: String,
                                                contentType: String,
                                                assignedBy: String,
                                                mloId: String,
                                                occurredOn: Long
                                              )
