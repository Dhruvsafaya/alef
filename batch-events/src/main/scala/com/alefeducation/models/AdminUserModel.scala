package com.alefeducation.models
import java.sql.Timestamp

object AdminUserModel {

  case class AdminDim(rel_admin_dw_id: Long,
                      admin_created_time: Timestamp,
                      admin_updated_time: Timestamp,
                      admin_deleted_time: Timestamp,
                      admin_dw_created_time: Timestamp,
                      admin_dw_updated_time: Timestamp,
                      admin_active_until: Timestamp,
                      admin_status: Int,
                      admin_id: String,
                      admin_dw_id: Long,
                      admin_avatar: String,
                      admin_onboarded: Boolean,
                      admin_school_dw_id: Long,
                      admin_role_dw_id: Long,
                      admin_expirable: Boolean,
                      admin_exclude_from_report: Boolean
                     )

  case class AdminRel(rel_admin_id: Long,
                      admin_created_time: Timestamp,
                      admin_updated_time: Timestamp,
                      admin_deleted_time: Timestamp,
                      admin_dw_created_time: Timestamp,
                      admin_dw_updated_time: Timestamp,
                      admin_active_until: Timestamp,
                      admin_status: Int,
                      admin_uuid: String,
                      admin_avatar: String,
                      admin_onboarded: String,
                      school_uuid: String,
                      role_uuid: String,
                      admin_expirable: String,
                      admin_exclude_from_report: String
                     )

  case class RoleDim(role_dw_id: Long, role_name: String)

  case class UserId(uuid: String)

  lazy val AdminUserRedshiftCols = Map(
    "uuid" -> "admin_uuid",
    "onboarded" -> "admin_onboarded",
    "school" -> "school_uuid",
    "expirable" -> "admin_expirable",
    "avatar" -> "admin_avatar",
    "occurredOn" -> "occurredOn",
    "admin_status" -> "admin_status",
    "role" -> "role_uuid",
    "excludeFromReport" -> "admin_exclude_from_report"
  )

  lazy val AdminUserDeletedDfCols = Map(
    "uuid" -> "admin_uuid",
    "admin_onboarded" -> "admin_onboarded",
    "school_uuid" -> "school_uuid",
    "admin_expirable" -> "admin_expirable",
    "admin_avatar" -> "admin_avatar",
    "occurredOn" -> "occurredOn",
    "admin_status" -> "admin_status",
    "role_uuid" -> "role_uuid",
    "excludeFromReport" -> "admin_exclude_from_report"
  )
}
