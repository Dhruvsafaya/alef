package com.alefeducation.schema.user

case class User(uuid: String,
                avatar: String,
                onboarded: Boolean,
                enabled: Boolean,
                school: TenantResource,
                role: String,
                roles: List[String],
                permissions: List[String],
                membershipsV3: List[Membership],
                occurredOn: Long,
                expirable: Boolean,
                excludeFromReport: Boolean)

case class TenantResource(uuid: String)

case class Membership(roleId: String,
                      role: String,
                      organization: String,
                      schoolId: String)

case class UserMovedBetweenSchools(uuid: String, role: String, sourceSchoolId: String, targetSchoolId: String, occurredOn: Long, membershipsV3: List[Membership])

case class UserDeleted(uuid: String, occurredOn: Long)

case class Heartbeat(uuid: String,
                     role: String,
                     channel: String,
                     schoolId: String,
                     occurredOn: Long)


case class UserAvatar(uuid: String, avatarId: String, gradeId: String, schoolId: String, occurredOn: Long, avatarType: String, avatarFileId: String)
