package com.alefeducation.schema.role


case class Role(id: String,
                name: String,
                description: String,
                predefined: Boolean,
                granularPermissions: List[String],
                created: String,
                updated: String,
                organization: Organization,
                roleType: String,
                categoryId: String,
                permissionToCategories: List[String],
                isCCLRole: Boolean
               )

case class Organization(name: String, code: String)
case class RoleEvent(id: String, name: String, occurredOn: Long, permissions: List[String], role: Role)


