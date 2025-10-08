package com.alefeducation.schema.pathwaytarget

case class PathwayTargetMutatedEvent(uuid: String,
                                     pathwayTargetId: String,
                                     pathwayId: String,
                                     classId: String,
                                     gradeId: String,
                                     schoolId: String,
                                     startDate: String,
                                     endDate: String,
                                     occurredOn: Long,
                                     teacherId: String,
                                     targetStatus: String)

case class StudentPathwayTargetMutatedEvent(uuid: String,
                                            pathwayTargetId: String,
                                            pathwayId: String,
                                            classId: String,
                                            gradeId: String,
                                            schoolId: String,
                                            studentTargetId: String,
                                            studentId: String,
                                            recommendedTarget: Int,
                                            finalizedTarget: Int,
                                            occurredOn: Long,
                                            teacherId: String,
                                            targetStatus: String,
                                            levelsCompleted: Int,
                                            earnedStars: Int)