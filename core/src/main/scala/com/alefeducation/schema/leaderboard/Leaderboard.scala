package com.alefeducation.schema.leaderboard

case class LeaderboardEvent(id: String,
                            `type`: String,
                             classId: String,
                             pathwayId: String,
                             gradeId: String,
                             className: String,
                             academicYearId: String,
                             startDate: String,
                             endDate: String,
                             leaders: List[Leader],
                             occurredOn: Long)

case class Leader(studentId: String,
                  avatar: String,
                  order: Int,
                  progress: Int,
                  averageScore: Double,
                  totalStars: Int)