package com.alefeducation.schema.team

case class TeamMutated(
    teamId: String,
    name: String,
    description: String,
    classId: String,
    teacherId: String,
    occurredOn: Long
)

case class TeamDeleted(
    teamId: String,
    occurredOn: Long
)

case class TeamMembersUpdated(
    teamId: String,
    students: List[String],
    occurredOn: Long
)
