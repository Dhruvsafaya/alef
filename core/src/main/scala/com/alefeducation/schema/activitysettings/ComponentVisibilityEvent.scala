package com.alefeducation.schema.activitysettings

case class ComponentVisibilityEvent(openPathEnabled: Boolean,
                         classId: String,
                         activityId: String,
                         teacherId: String,
                         schoolId: String,
                         subjectName: String,
                         gradeId: String,
                         gradeLevel: Int,
                         componentId: String,
                         componentStatus: String,
                         occurredOn: String)
