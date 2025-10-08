package com.alefeducation.schema.activitysettings

case class OpenPathEvent(classId: String,
                         activityId: String,
                         teacherId: String,
                         schoolId: String,
                         subjectName: String,
                         gradeId: String,
                         gradeLevel: String,
                         openPathEnabled: Boolean,
                         occurredOn: String)
