package com.alefeducation.schema.course

final case class WeeklyGoalMutatedEvent(weeklyGoalId: String,
                                        studentId: String,
                                        classId: String,
                                        goalId: String,
                                        occurredOn: Long)

final case class WeeklyGoalStarEvent(weeklyGoalId: String,
                                     studentId: String,
                                     classId: String,
                                     stars: Int,
                                     academicYearId: String,
                                     occurredOn: Long)

case class WeeklyProgressEvent(weeklyGoalId: String,
                               status: String,
                               completedActivity: String,
                               occurredOn: Long)
