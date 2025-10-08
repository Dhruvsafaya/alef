package com.alefeducation.models

import java.sql.Timestamp

object GuardianModel {
  case class GuardianCommon(occurredOn: Timestamp, status: Int, guardianId: String, studentId: String, invitationStatus: Int)
  case class DimGuardian(rel_guardian_dw_id: Int,
                         guardian_created_time: Timestamp,
                         guardian_updated_time: Timestamp,
                         guardian_deleted_time: Timestamp,
                         guardian_dw_created_time: Timestamp,
                         guardian_dw_updated_time: Timestamp,
                         guardian_active_until: Timestamp,
                         guardian_status: Int,
                         guardian_id: String,
                         guardian_dw_id: Int,
                         guardian_student_dw_id: Int,
                         guardian_invitation_status: Int)
  case class RelGuardian(rel_guardian_dw_id: Int,
                         guardian_created_time: Timestamp,
                         guardian_updated_time: Timestamp,
                         guardian_deleted_time: Timestamp,
                         guardian_dw_created_time: Timestamp,
                         guardian_dw_updated_time: Timestamp,
                         guardian_active_until: Timestamp,
                         guardian_status: Int,
                         guardian_id: String,
                         student_id: String,
                         guardian_invitation_status: Int)
}
