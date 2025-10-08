package com.alefeducation.models

import java.sql.Timestamp

object RoleModel {

  case class RoleUuid(role_name: String, role_uuid: String, role_status: Int, role_created_time: Timestamp)
}
