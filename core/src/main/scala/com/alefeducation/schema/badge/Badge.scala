package com.alefeducation.schema.badge

case class BadgeUpdated(id: String,
                        `type`: String,
                        order: Int,
                        title: String,
                        category: String,
                        tenantCode: String,
                        finalDescription: String,
                        createdAt: String,
                        k12Grades: List[String],
                        rules: List[Rule],
                        occurredOn: Long)

case class Rule(tier: String,
                order: String,
                defaultThreshold: String,
                defaultCompletionMessage: String,
                defaultDescription: String,
                rulesByGrade: List[RuleByGrade])

case class RuleByGrade(k12Grade: String,
                       threshold: String,
                       description: String,
                       completionMessage: String)

case class StudentBadgeAwarded(id: String,
                               studentId: String,
                               badgeType: String,
                               badgeTypeId: String,
                               tier: String,
                               nextChallengeDescription: String,
                               awardedMessage: String,
                               awardDescription: String,
                               academicYearId: String,
                               sectionId: String,
                               gradeId: String,
                               schoolId: String,
                               occurredOn: Long)