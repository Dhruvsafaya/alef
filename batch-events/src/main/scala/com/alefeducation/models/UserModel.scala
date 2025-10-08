package com.alefeducation.models

import java.sql.Timestamp

object UserModel {
  case class User(user_dw_id: Int, user_id: String, user_type: String, user_created_time: Timestamp, user_dw_created_time: Timestamp)
}
