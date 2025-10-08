package com.alefeducation.schema.guardian

case class GuardianJointActivityPendingEvent(guardianIds: List[String],
                                                   pathwayId: String,
                                                   pathwayLevelId: String,
                                                   classId: String,
                                                   studentId: String,
                                                   schoolId: String,
                                                   grade: Int,
                                                   occurredOn: Long
                                                  )
case class GuardianJointActivityStartedEvent(pathwayId: String,
                                                   pathwayLevelId: String,
                                                   classId: String,
                                                   studentId: String,
                                                   startedByGuardianId: String,
                                                   schoolId: String,
                                                   grade: Int,
                                                   attempt: Int,
                                                   assignedOn: Long,
                                                   occurredOn: Long
                                                  )
case class GuardianJointActivityCompletedEvent(pathwayId: String,
                                                     pathwayLevelId: String,
                                                     classId: String,
                                                     studentId: String,
                                                     schoolId: String,
                                                     grade: Int,
                                                     completedByGuardianId: String,
                                                     attempt: Int,
                                                     assignedOn: Long,
                                                     startedOn: Long,
                                                     occurredOn: Long
                                                    )
case class GuardianJointActivityRatedEvent(pathwayId: String,
                                                 pathwayLevelId: String,
                                                 classId: String,
                                                 studentId: String,
                                                 guardianId: String,
                                                 schoolId: String,
                                                 grade: Int,
                                                 attempt: Int,
                                                 rating: Int,
                                                 occurredOn: Long,
                                                )